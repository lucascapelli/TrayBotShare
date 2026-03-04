# ========================== run_sync.py (VERSÃO V5 TURBINADA - 8x MAIS RÁPIDA) ==========================
import json
import logging
import os
import random
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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


# ====================== DELAYS INTELIGENTES ======================
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

def _log_kv(key: str, value, indent: int = 4):
    logger.info("%s%s: %s", " " * indent, key, value)


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


# ====================== CACHE GLOBAL MULTI-KEY (MAIOR GANHO) ======================
DESTINO_CACHE: Dict[str, Any] = {}  # "name:xxx" → [candidatos], "ref/sku" → candidato único


@retry_on_fail(max_attempts=4, backoff=1.8)
def _preload_destino_cache(page: Any) -> bool:
    """V5 - Carrega tudo com page_size=500 + cache por ref/sku/nome"""
    global DESTINO_CACHE
    DESTINO_CACHE.clear()

    page_size = 500
    page_number = 1
    total_pages = 1
    loaded_count = 0

    logger.info("🚀 Pré-carregando cache DESTINO (V5 - multi-key + 500 por página)...")
    token = destino_page._extract_destino_token(page)  # função já existe no destino_page
    headers = {
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["Authorization"] = token

    while page_number <= total_pages:
        url = f"{config.DESTINO_BASE}/admin/api/products?page[size]={page_size}&page[number]={page_number}&sort=name"
        try:
            resp = page.request.get(url, headers=headers, timeout=45000)
            if resp.status != 200:
                logger.warning(f"Status {resp.status} na página {page_number}")
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
                    name_key = f"name:{norm_name}"
                    existing = DESTINO_CACHE.get(name_key)
                    if existing is None:
                        DESTINO_CACHE[name_key] = [data_item]
                    elif isinstance(existing, list):
                        existing.append(data_item)
                    else:
                        DESTINO_CACHE[name_key] = [existing, data_item]
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

    logger.info(f"✅ Cache carregado: {loaded_count} produtos | {len(DESTINO_CACHE)} chaves")
    return len(DESTINO_CACHE) > 100


# ====================== LOAD ORIGEM ======================
def _load_origem(context: Any, cookies_origem: list, origem_url: str, source_user: str, source_pass: str, storage_origem=None) -> List[dict]:
    if storage_origem is not None and hasattr(storage_origem, "read_all"):
        try:
            produtos = storage_origem.read_all()
            if isinstance(produtos, list) and produtos:
                logger.info("📦 ORIGEM carregada do storage: %d produto(s)", len(produtos))
                return produtos
        except Exception as exc:
            logger.warning("⚠️ Falha ao ler storage: %s", exc)

    fallback_path = os.path.join("produtos", "ProdutosOrigem.json")
    if os.path.isfile(fallback_path):
        try:
            with open(fallback_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            if isinstance(data, list):
                logger.info("📄 ORIGEM carregada do JSON local: %d produto(s)", len(data))
                return data
        except Exception as exc:
            logger.warning("⚠️ Falha JSON local: %s", exc)

    logger.error("❌ ORIGEM não disponível")
    return []


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
    print("🔄 SYNC v5 — TURBINADO (cache multi-key + batch)")
    print("═" * 80)

    _log_section("ETAPA 1: Carregando ORIGEM")
    produtos = _load_origem(
        context, cookies_origem, origem_url, source_user, source_pass, storage_origem
    )
    if not produtos:
        print("❌ Nenhum produto na ORIGEM.")
        return

    # Rate limit + filtro teste
    if getattr(config, "RATE_LIMIT", 0) > 0 and len(produtos) > config.RATE_LIMIT:
        produtos = produtos[:config.RATE_LIMIT]
    if config.MODO_TESTE_APENAS_COM_INFOS:
        produtos = [p for p in produtos if p.get("informacoes_adicionais")]

    # ====================== PRÉ-CARREGAMENTO DESTINO ======================
    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta.")
        return
    page = pages[0]

    cache_ok = _preload_destino_cache(page)
    if not cache_ok:
        logger.warning("⚠️ Cache não carregou completamente, mas continuando...")

    # ====================== MATCHING V5 (uma única vez) ======================
    _log_section("ETAPA 2: MATCHING V5 - BATCH (cache + browser)")
    all_matches = destino_page.match_products_inteligente(
        page=page,
        origem_products=produtos,
        destino_cache=DESTINO_CACHE,
        logger=logger,
        short_delay=_short_delay,
    )

    if not all_matches:
        print("❌ Nenhum produto encontrado no destino.")
        return

    # ====================== PROCESSAMENTO FINAL ======================
    _log_section("ETAPA 3: PROCESSANDO PRODUTOS")
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
            logger.warning("⛔ Produto DESTINO ID %s bloqueado por configuração", pid)
            continue

        if str(pid) in processed_destino_ids:
            origem_anterior = origem_por_destino_id.get(str(pid), "")
            origem_atual = (origem_prod.get("nome") or "")
            logger.warning(
                "⚠️ Colisão de match: DESTINO ID %s já usado por '%s' e também caiu para '%s' — pulando duplicado",
                pid,
                origem_anterior[:90],
                origem_atual[:90],
            )
            continue

        if origem_key in completed_origem_keys:
            continue

        processed_destino_ids.add(str(pid))
        origem_por_destino_id[str(pid)] = (origem_prod.get("nome") or "")
        completed_origem_keys.add(origem_key)

        logger.info(f"[{processed_count+1}/{target_count}] Processando → {nome[:70]}")

        _short_delay()

        destino_json, token = destino_page.fetch_product_and_token(page, pid, logger)
        log_entry = {
            "destino_id": pid,
            "destino_name": nome,
            "origem_nome": (origem_prod.get("nome") or ""),
        }

        if not destino_json or not token:
            logger.error(f"❌ Falha ao buscar JSON do produto {pid} — pulando")
            log_entry["status"] = "erro_json_token"
            failed_count += 1
            _save_log(log_entry)
            continue

        try:
            payload = domain.build_product_payload(origem_prod, destino_json)
            ok, status, body = destino_api.put_product(page, pid, payload, token)

            if not ok:
                logger.error("❌ PUT falhou (ID %s, status %d): %s", pid, status, (body or "")[:200])
                log_entry["status"] = "erro_put"
                log_entry["put_status"] = "falha"
                log_entry["put_http_status"] = status
                log_entry["put_erro"] = (body or "")[:300]
                failed_count += 1
                _save_log(log_entry)
                continue

            log_entry["put_status"] = "sucesso"
            log_entry["put_http_status"] = status

            sync_additional_infos(
                page,
                pid,
                origem_prod.get("informacoes_adicionais", []),
                token,
                log_entry,
                short_delay=_short_delay,
                medium_delay=_medium_delay,
            )

            sync_variants(
                page,
                pid,
                origem_prod,
                token,
                log_entry,
                short_delay=_short_delay,
                medium_delay=_medium_delay,
            )

            log_entry["status"] = "sucesso"
            _save_log(log_entry)

        except Exception as exc:
            logger.error("❌ Erro processando produto %s: %s", pid, exc)
            log_entry["status"] = "erro_execucao"
            log_entry["erro"] = str(exc)
            failed_count += 1
            _save_log(log_entry)
            continue

        processed_count += 1
        logger.info(f"✅ Progresso: {processed_count}/{target_count} | {nome}")

        if processed_count % 5 == 0:
            _medium_delay()
        else:
            _short_delay()

    print("\n" + "═" * 80)
    print(f"✅ SYNC CONCLUÍDO — {processed_count}/{target_count} produtos processados")
    if failed_count:
        print(f"❌ Falhas: {failed_count}")
    if skipped_blocked_count:
        print(f"⛔ Pulados por bloqueio: {skipped_blocked_count}")
    print("═" * 80)