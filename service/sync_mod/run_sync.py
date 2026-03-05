# ========================== run_sync.py (V8 — FIX OPÇÕES + COOKIES) ==========================
import json
import logging
import os
import random
import time
import requests
from typing import Any, List, Dict, Optional
from service.auth import authenticate
from service.auth import load_storage_state, _resolve_state_path
from service.sync_mod import config
from service.sync_mod import destino_api
from service.sync_mod import destino_page
from service.sync_mod import domain
from service.sync_mod.services.additional_info_sync import sync_additional_infos
from service.sync_mod.services.variant_sync import sync_variants

logger = logging.getLogger("sync")


# ====================== RETRY DECORATOR ======================
def retry_on_fail(max_attempts=3, backoff=1.5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        logger.error("❌ Falha definitiva após %d tentativas: %s", max_attempts, e)
                        raise
                    wait = backoff ** attempt
                    logger.warning("⚠️ Tentativa %d/%d falhou. Aguardando %.1fs...", attempt, max_attempts, wait)
                    time.sleep(wait)
            return None
        return wrapper
    return decorator


# ====================== DELAYS ======================
def _human_delay(min_s: float = 1.2, max_s: float = 3.0):
    mid = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mid, std)))
    time.sleep(delay)

def _short_delay():
    _human_delay(0.6, 1.4)

def _medium_delay():
    _human_delay(1.5, 2.8)

def _long_delay():
    _human_delay(2.8, 5.0)


def _log_section(title: str):
    logger.info("\n" + "─" * 70)
    logger.info(" %s", title)
    logger.info("─" * 70)


def _save_log(log_entry: dict):
    try:
        os.makedirs(os.path.dirname(config.LOG_FILE), exist_ok=True)

        existing = []
        if os.path.isfile(config.LOG_FILE):
            try:
                with open(config.LOG_FILE, "r", encoding="utf-8") as file:
                    content = file.read().strip()
                    if content:
                        existing = json.loads(content)
                        if not isinstance(existing, list):
                            existing = [existing]
            except (json.JSONDecodeError, ValueError):
                existing = []

        existing.append(log_entry)
        with open(config.LOG_FILE, "w", encoding="utf-8") as file:
            json.dump(existing, file, indent=2, ensure_ascii=False)
    except Exception as exc:
        logger.warning("Erro ao salvar log: %s", exc)


def _origem_product_key(produto: dict) -> str:
    if not isinstance(produto, dict) or not produto:
        return "invalid:none"
    produto_id = str(produto.get("produto_id") or produto.get("id") or "").strip()
    if produto_id and produto_id.isdigit() and produto_id != "0":
        return f"id:{produto_id}"
    referencia = str(produto.get("reference") or produto.get("referencia") or produto.get("sku") or "").strip()
    if referencia:
        return f"ref:{referencia.lower()}"
    nome = str(produto.get("nome") or "").strip()
    if nome:
        nome_lower = nome.lower()[:80]
        nome_hash = str(abs(hash(nome_lower)))[:10]
        return f"nome:{nome_lower}|{nome_hash}"
    return f"hash:{id(produto)}"


# ====================== CACHE ======================
DESTINO_CACHE: Dict[str, Any] = {}


@retry_on_fail(max_attempts=4, backoff=1.8)
def _preload_destino_cache(page: Any) -> bool:
    global DESTINO_CACHE
    DESTINO_CACHE.clear()

    page_size = 500
    page_number = 1
    total_pages = 1
    loaded_count = 0

    logger.info("🚀 Pré-carregando cache DESTINO...")
    token = destino_page._extract_destino_token(page)
    headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
    if token:
        headers["Authorization"] = token

    while page_number <= total_pages:
        url = f"{config.DESTINO_BASE}/admin/api/products?page[size]={page_size}&page[number]={page_number}&sort=name"
        try:
            resp = page.request.get(url, headers=headers, timeout=45000)
            if resp.status != 200:
                break
            data = resp.json()
            items = data.get("data") or []
            for item in items:
                item_id = str(item.get("id") or "")
                if not item_id:
                    continue
                name = (item.get("name") or "").strip()
                ref = str(item.get("reference") or item.get("referencia") or "").strip()
                sku = str(item.get("sku") or "").strip()
                norm_name = destino_page.normalize_name(name)
                data_item = {"id": item_id, "name": name}
                if norm_name:
                    key = f"name:{norm_name}"
                    existing = DESTINO_CACHE.get(key)
                    if existing is None:
                        DESTINO_CACHE[key] = [data_item]
                    elif isinstance(existing, list):
                        existing.append(data_item)
                    else:
                        DESTINO_CACHE[key] = [existing, data_item]
                if ref:
                    DESTINO_CACHE[f"ref:{ref.lower()}"] = data_item
                if sku:
                    DESTINO_CACHE[f"sku:{sku.lower()}"] = data_item
                loaded_count += 1

            paging = data.get("paging") or {}
            total = int(paging.get("total") or 0)
            total_pages = max(1, (total + page_size - 1) // page_size)
            page_number += 1
            if not items:
                break
        except Exception as e:
            logger.error(f"Erro preload página {page_number}: {e}")
            break

    logger.info(f"✅ Cache: {loaded_count} produtos | {len(DESTINO_CACHE)} chaves")
    return len(DESTINO_CACHE) > 100


# ====================== LOAD ORIGEM ======================
def _load_origem(context, cookies_origem, origem_url, source_user, source_pass, storage_origem=None):
    origem_source = str(getattr(config, "ORIGEM_SOURCE", "file") or "file").strip().lower()

    def _read_storage():
        if storage_origem and hasattr(storage_origem, "read_all"):
            try:
                produtos = storage_origem.read_all()
                if isinstance(produtos, list) and produtos:
                    logger.info("📦 ORIGEM do storage: %d produto(s)", len(produtos))
                    return produtos
            except Exception as exc:
                logger.warning("⚠️ Falha storage: %s", exc)
        return []

    def _read_file():
        path = os.path.join("produtos", "ProdutosOrigem.json")
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    logger.info("📄 ORIGEM do JSON: %d produto(s)", len(data))
                    return data
            except Exception as exc:
                logger.warning("⚠️ Falha JSON: %s", exc)
        return []

    if origem_source == "tray_api":
        return _read_storage() or _read_file()
    return _read_file() or _read_storage() or []


# ══════════════════════════════════════════════════════════════════════
# _is_additional_infos_model — Considera dados da ORIGEM
# ══════════════════════════════════════════════════════════════════════
def _is_additional_infos_model(destino_json: dict, origem_prod: dict = None) -> bool:
    if not isinstance(destino_json, dict):
        return False
    has_variation = str(destino_json.get("has_variation", ""))
    properties_len = len(destino_json.get("Properties") or [])
    additional_infos_len = len(destino_json.get("AdditionalInfos") or [])

    if has_variation == "1" and properties_len > 0:
        return False
    if additional_infos_len > 0:
        return True
    if origem_prod and isinstance(origem_prod, dict):
        origem_infos = _get_origem_infos(origem_prod)
        origem_vars = origem_prod.get("variacoes") or []
        if (origem_infos or origem_vars) and has_variation != "1" and properties_len == 0:
            logger.info("🔍 Destino vazio + origem tem dados → modelo AdditionalInfos")
            return True
    return False


# ══════════════════════════════════════════════════════════════════════
# _get_origem_infos — Aceita formato Tray API e formato interno
# ══════════════════════════════════════════════════════════════════════
def _get_origem_infos(origem_prod: dict) -> list:
    # Formato interno
    infos = origem_prod.get("informacoes_adicionais") or []
    if infos and isinstance(infos, list):
        first = infos[0] if infos else {}
        if isinstance(first, dict) and (first.get("nome") or first.get("opcoes")):
            return infos

    # Formato Tray API
    tray_infos = origem_prod.get("AdditionalInfos") or []
    if tray_infos and isinstance(tray_infos, list):
        converted = _convert_tray_infos(tray_infos)
        if converted:
            return converted

    # Formato interno com chaves Tray
    if infos and isinstance(infos, list):
        first = infos[0] if infos else {}
        if isinstance(first, dict) and (first.get("name") or first.get("options")):
            return _convert_tray_infos(infos)

    return infos if isinstance(infos, list) else []


def _convert_tray_infos(tray_infos: list) -> list:
    result = []
    for info in tray_infos:
        if not isinstance(info, dict):
            continue
        nome = (info.get("name") or info.get("nome") or "").strip()
        if not nome:
            continue
        info_type = (info.get("type") or "").strip().lower()
        opcoes = []
        for opt in (info.get("options") or info.get("opcoes") or []):
            if not isinstance(opt, dict):
                continue
            opt_nome = (opt.get("name") or opt.get("nome") or "").strip()
            if opt_nome:
                opcoes.append({"nome": opt_nome, "valor": str(opt.get("value") or opt.get("valor") or "0.00")})
        result.append({"nome": nome, "tipo": info_type or "select", "opcoes": opcoes})
    return result


def run_sync(
    context: Any,
    storage_origem=None,
    storage_destino=None,
    origem_url: str = "",
    source_user: str = "",
    source_pass: str = "",
    cookies_origem: list = None,
):
    print("\n" + "═" * 80)
    print("🔄 SYNC v8 — FIX OPÇÕES CHECKED + COOKIES")
    print("═" * 80)

    _log_section("ETAPA 1: Carregando ORIGEM")
    produtos = _load_origem(context, cookies_origem, origem_url, source_user, source_pass, storage_origem)
    if not produtos:
        print("❌ Nenhum produto na ORIGEM.")
        return

    if getattr(config, "RATE_LIMIT", 0) > 0 and len(produtos) > config.RATE_LIMIT:
        produtos = produtos[:config.RATE_LIMIT]
    if config.MODO_TESTE_APENAS_COM_INFOS:
        produtos = [p for p in produtos if _get_origem_infos(p)]

    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta.")
        return
    page = pages[0]

    cache_ok = _preload_destino_cache(page)
    if not cache_ok:
        logger.warning("⚠️ Cache incompleto, continuando...")

    # ══════════════════════════════════════════════════════════════════════
    # Verificar acesso à ORIGEM para leitura de opções checked
    # ══════════════════════════════════════════════════════════════════════
    origin_base = getattr(config, "ORIGEM_TRAY_BASE", "") or ""
    has_origin_access = bool(origin_base and cookies_origem)

    if has_origin_access:
        # Debug: verificar tipo dos cookies
        sample = cookies_origem[0] if cookies_origem else None
        cookie_type = type(sample).__name__
        logger.info(
            "🔗 ORIGEM: %s | %d cookies (tipo: %s)",
            origin_base, len(cookies_origem), cookie_type,
        )
        # Mostrar header que será construído
        test_header = destino_api._build_cookie_header(cookies_origem)
        logger.info("    Cookie header: %s...(%d chars)", test_header[:80], len(test_header))
    else:
        logger.warning(
            "⚠️ Sem acesso à ORIGEM (origin_base=%s, cookies=%s). "
            "Opções serão do JSON (PODE MARCAR TODAS EM VEZ DAS SELECIONADAS).",
            bool(origin_base), bool(cookies_origem),
        )

    _log_section("ETAPA 2: MATCHING")
    all_matches = destino_page.match_products_inteligente(
        page=page,
        origem_products=produtos,
        destino_cache=DESTINO_CACHE,
        logger=logger,
        short_delay=_short_delay,
    )
    if not all_matches:
        print("❌ Nenhum match encontrado.")
        return

    _log_section("ETAPA 3: PROCESSAMENTO")
    processed_count = 0
    failed_count = 0
    skipped_blocked_count = 0
    target_count = len(all_matches)
    blocked_ids = {str(item) for item in getattr(config, "SKIP_DESTINO_PRODUCT_IDS", set())}
    processed_destino_ids = set()
    origem_por_destino_id = {}
    completed_origem_keys = set()

    for match in all_matches:
        pid = match["destino_id"]
        nome = match["destino_name"]
        origem_prod = match["origem_product"]
        origem_key = _origem_product_key(origem_prod)

        if str(pid) in blocked_ids:
            skipped_blocked_count += 1
            continue
        if str(pid) in processed_destino_ids:
            continue
        if origem_key in completed_origem_keys:
            continue

        processed_destino_ids.add(str(pid))
        origem_por_destino_id[str(pid)] = (origem_prod.get("nome") or "")
        completed_origem_keys.add(origem_key)

        logger.info(f"[{processed_count+1}/{target_count}] → {nome[:70]}")
        _short_delay()

        destino_json, token = destino_page.fetch_product_and_token(page, pid, logger)
        log_entry = {
            "destino_id": pid,
            "destino_name": nome,
            "origem_nome": (origem_prod.get("nome") or ""),
        }

        if not destino_json or not token:
            logger.error(f"❌ Falha JSON/token produto {pid}")
            log_entry["status"] = "erro_json_token"
            failed_count += 1
            destino_page._append_live_result(destino_page.ENCONTRADOS_PATH, origem_prod.get("nome") or nome)
            _save_log(log_entry)
            continue

        try:
            payload = domain.build_product_payload(origem_prod, destino_json)
            ok, status, body = destino_api.put_product(page, pid, payload, token)

            if not ok:
                logger.error("❌ PUT falhou (ID %s, status %d): %s", pid, status, (body or "")[:200])
                log_entry["status"] = "erro_put"
                log_entry["put_http_status"] = status
                failed_count += 1
                destino_page._append_live_result(destino_page.ENCONTRADOS_PATH, origem_prod.get("nome") or nome)
                _save_log(log_entry)
                continue

            log_entry["put_status"] = "sucesso"
            log_entry["put_http_status"] = status

            # ══════════════════════════════════════════════════════════
            # Infos + opções
            # ══════════════════════════════════════════════════════════
            infos_origem = _get_origem_infos(origem_prod)
            variacoes_origem = origem_prod.get("variacoes", []) or []
            destino_is_infos_model = _is_additional_infos_model(destino_json, origem_prod)
            infos_from_variacoes_mode = bool(destino_is_infos_model and variacoes_origem)

            if infos_from_variacoes_mode:
                infos_to_sync = domain.build_infos_for_additional_model(origem_prod)
                source_context = "origem_infos+variacoes"
            else:
                infos_to_sync = infos_origem
                source_context = "origem_infos"

            should_create_fields = bool(infos_to_sync)

            # ══════════════════════════════════════════════════════════
            # Ler opções CHECKED da ORIGEM via HTTP
            # ══════════════════════════════════════════════════════════
            origin_checked_options = None
            origem_product_id = str(
                origem_prod.get("produto_id") or origem_prod.get("id") or ""
            ).strip()

            if has_origin_access and origem_product_id and infos_to_sync:
                logger.info("📖 Lendo opções checked da ORIGEM (produto %s)...", origem_product_id)
                _short_delay()
                try:
                    origin_checked_options = destino_api.read_origin_checked_options(
                        page=page,
                        origin_base=origin_base,
                        product_id=origem_product_id,
                        cookies_origem=cookies_origem,
                        logger=logger,
                    )
                    if origin_checked_options:
                        total_opts = sum(len(v) for v in origin_checked_options.values())
                        logger.info("✅ ORIGEM: %d campos, %d opções checked", len(origin_checked_options), total_opts)
                    else:
                        logger.warning("⚠️ Não conseguiu ler opções da ORIGEM → fallback JSON")
                except Exception as exc:
                    logger.warning("⚠️ Erro lendo ORIGEM: %s → fallback", exc)
                    origin_checked_options = None

            logger.info(
                "📋 Infos: %d | create=%s | source=%s | options=%s",
                len(infos_to_sync), should_create_fields, source_context,
                "ORIGEM_HTML" if origin_checked_options else "JSON_FALLBACK",
            )

            sync_additional_infos(
                page,
                pid,
                infos_to_sync,
                token,
                log_entry,
                short_delay=_short_delay,
                medium_delay=_medium_delay,
                create_missing_fields=should_create_fields,
                source_context=source_context,
                origin_checked_options=origin_checked_options,
            )

            sync_variants(
                page, pid, origem_prod, token, log_entry,
                short_delay=_short_delay, medium_delay=_medium_delay,
                infos_already_synced=infos_from_variacoes_mode,
            )

            destino_page.append_encontrado_sincronizado(origem_prod.get("nome") or nome)
            log_entry["status"] = "sucesso"
            _save_log(log_entry)

        except Exception as exc:
            logger.error("❌ Erro produto %s: %s", pid, exc)
            log_entry["status"] = "erro_execucao"
            log_entry["erro"] = str(exc)
            failed_count += 1
            destino_page._append_live_result(destino_page.ENCONTRADOS_PATH, origem_prod.get("nome") or nome)
            _save_log(log_entry)
            continue

        processed_count += 1
        logger.info(f"✅ {processed_count}/{target_count} | {nome}")

        if processed_count % 5 == 0:
            _medium_delay()
        else:
            _short_delay()

    print(f"\n{'═' * 80}")
    print(f"✅ SYNC CONCLUÍDO — {processed_count}/{target_count}")
    if failed_count:
        print(f"❌ Falhas: {failed_count}")
    if skipped_blocked_count:
        print(f"⛔ Bloqueados: {skipped_blocked_count}")
    print("═" * 80)