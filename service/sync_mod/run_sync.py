import json
import logging
import os
import random
import time
from typing import Any, List

from service.sync_mod import config
from service.sync_mod import destino_api
from service.sync_mod import destino_page
from service.sync_mod import domain
from service.sync_mod.services.additional_info_sync import sync_additional_infos
from service.sync_mod.services.variant_sync import sync_variants

logger = logging.getLogger("sync")


def _human_delay(min_s: float = 2.0, max_s: float = 5.0):
    mid = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mid, std)))
    time.sleep(delay)


def _short_delay():
    _human_delay(1.0, 2.5)


def _medium_delay():
    _human_delay(2.5, 5.0)


def _long_delay():
    _human_delay(4.0, 8.0)


def _log_section(title: str):
    logger.info("")
    logger.info("─" * 60)
    logger.info("  %s", title)
    logger.info("─" * 60)


def _log_kv(key: str, value, indent: int = 4):
    logger.info("%s%s: %s", " " * indent, key, value)


def _load_origem() -> List[dict]:
    if not os.path.isfile(config.ORIGEM_JSON_PATH):
        logger.error("❌ Arquivo não encontrado: %s", config.ORIGEM_JSON_PATH)
        return []
    try:
        with open(config.ORIGEM_JSON_PATH, "r", encoding="utf-8") as file:
            produtos = json.load(file)
        logger.info("📦 %d produtos carregados da ORIGEM", len(produtos))
        return produtos
    except Exception as exc:
        logger.error("❌ Erro ao carregar ORIGEM: %s", exc)
        return []


def _filter_com_infos(produtos: List[dict]) -> List[dict]:
    com_infos = [p for p in produtos if p.get("informacoes_adicionais")]
    logger.info("🔍 MODO TESTE: %d/%d com informações adicionais", len(com_infos), len(produtos))
    return com_infos


def _save_log(log_entry: dict):
    try:
        os.makedirs(os.path.dirname(config.LOG_FILE), exist_ok=True)

        existing = []
        if os.path.isfile(config.LOG_FILE):
            with open(config.LOG_FILE, "r", encoding="utf-8") as file:
                existing = json.load(file)
                if not isinstance(existing, list):
                    existing = [existing]

        existing.append(log_entry)
        with open(config.LOG_FILE, "w", encoding="utf-8") as file:
            json.dump(existing, file, indent=2, ensure_ascii=False)
        logger.info("📋 Log salvo: %s", config.LOG_FILE)
    except Exception as exc:
        logger.warning("Erro ao salvar log: %s", exc)


def run_sync(
    context: Any,
    storage_origem=None,
    storage_destino=None,
    origem_url: str = "",
    source_user: str = "",
    source_pass: str = "",
    cookies_origem: list = None,
):
    print("\n" + "═" * 70)
    print("🔄 SYNC: ORIGEM → DESTINO (v2 — modular)")
    print(f"   Rate limit: {config.RATE_LIMIT} produto(s) por execução")
    if config.MODO_TESTE_APENAS_COM_INFOS:
        print("   ⚙️  MODO TESTE: apenas produtos COM informações adicionais")
    print("═" * 70)

    _log_section("ETAPA 1: Carregando ORIGEM")
    produtos = _load_origem()
    if not produtos:
        print("❌ Nenhum produto na ORIGEM.")
        return

    if config.MODO_TESTE_APENAS_COM_INFOS:
        produtos = _filter_com_infos(produtos)
        if not produtos:
            print("❌ Nenhum produto com informações adicionais.")
            return

        for produto in produtos[:5]:
            nome = (produto.get("nome") or "")[:50]
            infos = [i.get("nome", "?") for i in produto.get("informacoes_adicionais", [])]
            variacoes_count = len(produto.get("variacoes", []))
            logger.info("    • %s → infos=%s, variações=%d", nome, infos, variacoes_count)

    _log_section("ETAPA 2: Buscando match no DESTINO")
    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta no contexto.")
        return
    page = pages[0]

    blocked_ids = {str(item) for item in getattr(config, "SKIP_DESTINO_PRODUCT_IDS", set())}
    remaining_products = list(produtos)
    match = None

    while remaining_products:
        candidate = destino_page.find_one_match(
            page,
            remaining_products,
            human_delay=_human_delay,
            short_delay=_short_delay,
            logger=logger,
        )

        if not candidate:
            break

        candidate_id = str(candidate.get("destino_id", ""))
        if candidate_id in blocked_ids:
            logger.warning("⛔ Produto DESTINO ID %s está bloqueado para sync; buscando próximo", candidate_id)
            origem_candidate = candidate.get("origem_product")
            remaining_products = [p for p in remaining_products if p is not origem_candidate]
            continue

        match = candidate
        break

    if not match:
        if blocked_ids:
            print(f"❌ Nenhum match válido encontrado (IDs bloqueados: {sorted(blocked_ids)}).")
        else:
            print("❌ Nenhum match encontrado.")
        return

    pid = match["destino_id"]
    nome = match["destino_name"]
    origem_prod = match["origem_product"]

    print(f"\n✅ Match: '{nome}' → ID {pid}")

    log_entry = {
        "destino_id": pid,
        "destino_name": nome,
        "origem_nome": (origem_prod.get("nome") or ""),
    }

    _log_section("ETAPA 3: Capturando dados do DESTINO")
    _medium_delay()

    destino_json, token = destino_page.fetch_product_and_token(page, pid, logger=logger)

    if not destino_json:
        print(f"❌ Não conseguiu capturar JSON do produto {pid}")
        log_entry["status"] = "erro_json"
        _save_log(log_entry)
        return

    if not token:
        print("❌ Token não capturado — impossível prosseguir com API calls")
        log_entry["status"] = "erro_token"
        _save_log(log_entry)
        return

    logger.info("📄 Estado atual no DESTINO:")
    _log_kv("name", destino_json.get("name"))
    _log_kv("price", destino_json.get("price"))
    _log_kv("stock", destino_json.get("stock"))
    _log_kv("reference", destino_json.get("reference"))
    _log_kv("url", destino_json.get("url", {}).get("https", "N/A") if isinstance(destino_json.get("url"), dict) else "N/A")
    _log_kv("AdditionalInfos", len(destino_json.get("AdditionalInfos", [])))

    _log_section("ETAPA 4: PUT dados básicos")

    payload = domain.build_product_payload(origem_prod, destino_json)

    logger.info("📤 Payload PUT:")
    for key, value in payload.items():
        value_str = str(value)[:80] if not isinstance(value, (list, dict)) else json.dumps(value, ensure_ascii=False)[:80]
        logger.info("    %s: %s", key, value_str)

    _medium_delay()
    ok, status, body = destino_api.put_product(page, pid, payload, token)

    if ok:
        logger.info("✅ PUT sucesso (status %d)", status)
        log_entry["put_status"] = "sucesso"
        log_entry["put_http_status"] = status
    else:
        logger.error("❌ PUT falhou (status %d): %s", status, body[:300])
        log_entry["put_status"] = "falha"
        log_entry["put_http_status"] = status
        log_entry["put_erro"] = body[:300]

    _long_delay()
    sync_additional_infos(
        page,
        pid,
        origem_prod.get("informacoes_adicionais", []),
        token,
        log_entry,
        short_delay=_short_delay,
        medium_delay=_medium_delay,
    )

    _long_delay()
    sync_variants(
        page,
        pid,
        origem_prod,
        token,
        log_entry,
        short_delay=_short_delay,
        medium_delay=_medium_delay,
    )

    print("\n" + "═" * 70)
    print("✅ SYNC FINALIZADO")
    print(f"   Produto: [{pid}] {nome}")
    print(f"   PUT: {log_entry.get('put_status', 'N/A')}")

    infos_status = log_entry.get("infos_adicionais", {}).get("status", "N/A")
    print(f"   Infos Adicionais: {infos_status}")

    var_info = log_entry.get("variacoes", {})
    if var_info.get("status") == "sem_variacoes_origem":
        print("   Variações: sem variações na ORIGEM")
    else:
        print(
            f"   Variações: match={var_info.get('match', 0)}, "
            f"criadas={var_info.get('criadas', 0)}, "
            f"deletadas={var_info.get('deletadas', 0)}"
        )

    if config.MODO_TESTE_APENAS_COM_INFOS:
        print("\n   ℹ️ MODO TESTE ativo — setar MODO_TESTE_APENAS_COM_INFOS = False pra rodar todos")

    print("═" * 70)

    _save_log(log_entry)
