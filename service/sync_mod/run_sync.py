#run_sync.py
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

# ====================== RETRY ======================
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
                with open(config.LOG_FILE, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        existing = json.loads(content)
                        if not isinstance(existing, list):
                            existing = [existing]
            except (json.JSONDecodeError, ValueError):
                existing = []
        existing.append(log_entry)
        with open(config.LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
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

def _get_origem_infos(origem_prod: dict) -> list:
    return domain._get_infos_from_product(origem_prod)

def _get_origem_variacoes(origem_prod: dict) -> list:
    return domain._get_variacoes_from_product(origem_prod)

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
        origem_vars = _get_origem_variacoes(origem_prod)
        if (origem_infos or origem_vars) and has_variation != "1" and properties_len == 0:
            logger.info(
                "🔍 Destino vazio + ORIGEM tem %d infos + %d variações → modelo AdditionalInfos",
                len(origem_infos), len(origem_vars),
            )
            return True
    return False

def _force_additional_infos_for_rings(destino_json: dict, origem_prod: dict) -> bool:
    nome = str(
        (destino_json or {}).get("name") or 
        (origem_prod or {}).get("nome") or ""
    ).lower()
    categoria = str(
        (destino_json or {}).get("category_name") or 
        (origem_prod or {}).get("categoria_name") or 
        (origem_prod or {}).get("categoria") or ""
    ).upper()
    
    if any(palavra in nome for palavra in ["anel", "anéis", "personalizado prata", "banho de ouro"]):
        logger.info("🛡️ FORÇANDO Additional Infos model (produto de anel detectado)")
        return True
    
    if categoria in ("ANÉIS", "ANEL", "JOIAS PERSONALIZADAS"):
        logger.info("🛡️ FORÇANDO Additional Infos model (categoria de anéis detectada)")
        return True
        
    return False

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
    print("🔄 SYNC v9 — FIX VARIAÇÕES→ADDITIONAL INFOS + ANÉIS FORÇADOS")
    print("═" * 80)
    
    _log_section("ETAPA 1: Carregando ORIGEM")
    produtos = _load_origem(context, cookies_origem, origem_url, source_user, source_pass, storage_origem)
    if not produtos:
        print("❌ Nenhum produto na ORIGEM.")
        return
    
    if produtos:
        sample = produtos[0]
        keys = sorted(sample.keys()) if isinstance(sample, dict) else []
        logger.info("📋 Campos no primeiro produto da ORIGEM: %s", keys)
        has_variant = sum(1 for p in produtos[:50] if isinstance(p, dict) and (p.get("Variant") or p.get("variacoes")))
        has_infos = sum(1 for p in produtos[:50] if isinstance(p, dict) and (p.get("AdditionalInfos") or p.get("informacoes_adicionais")))
        logger.info("📊 Dos primeiros 50 produtos: %d com variações, %d com infos adicionais", has_variant, has_infos)
    
    if getattr(config, "RATE_LIMIT", 0) > 0 and len(produtos) > config.RATE_LIMIT:
        produtos = produtos[:config.RATE_LIMIT]
    
    if config.MODO_TESTE_APENAS_COM_INFOS:
        produtos = [p for p in produtos if _get_origem_infos(p) or _get_origem_variacoes(p)]
    
    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta.")
        return
    page = pages[0]
    
    cache_ok = _preload_destino_cache(page)
    if not cache_ok:
        logger.warning("⚠️ Cache incompleto, continuando...")
    
    origin_base = getattr(config, "ORIGEM_TRAY_BASE", "") or ""
    has_origin_access = bool(origin_base and cookies_origem)
    if has_origin_access:
        cookie_header = destino_api._build_cookie_header(cookies_origem)
        logger.info("🔗 ORIGEM: %s | cookie: %s...(%d chars)", origin_base, cookie_header[:60], len(cookie_header))
    else:
        logger.warning("⚠️ Sem acesso à ORIGEM para ler opções checked")
    
    _log_section("ETAPA 2: MATCHING")
    all_matches = destino_page.match_products_inteligente(
        page=page, origem_products=produtos, destino_cache=DESTINO_CACHE,
        logger=logger, short_delay=_short_delay,
    )
    if not all_matches:
        print("❌ Nenhum match.")
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
        if str(pid) in processed_destino_ids or origem_key in completed_origem_keys:
            continue
        
        processed_destino_ids.add(str(pid))
        origem_por_destino_id[str(pid)] = (origem_prod.get("nome") or "")
        completed_origem_keys.add(origem_key)
        
        logger.info(f"[{processed_count+1}/{target_count}] → {nome[:70]}")
        _short_delay()
        
        destino_json, token = destino_page.fetch_product_and_token(page, pid, logger)
        log_entry = {"destino_id": pid, "destino_name": nome, "origem_nome": (origem_prod.get("nome") or "")}
        
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
            
            infos_origem = _get_origem_infos(origem_prod)
            variacoes_origem = _get_origem_variacoes(origem_prod)
            logger.info(
                "📦 Produto ORIGEM: %d infos adicionais, %d variações",
                len(infos_origem), len(variacoes_origem),
            )
            
            if variacoes_origem:
                for i, v in enumerate(variacoes_origem[:3]):
                    sku_items = domain._extract_sku_items_from_variant(v)
                    logger.info(" var[%d]: %s", i, sku_items)
            
            destino_is_infos_model = (
                _is_additional_infos_model(destino_json, origem_prod) or
                _force_additional_infos_for_rings(destino_json, origem_prod)
            )
            infos_from_variacoes_mode = bool(destino_is_infos_model and variacoes_origem)
            
            if infos_from_variacoes_mode:
                infos_to_sync = domain.build_infos_for_additional_model(origem_prod)
                source_context = "origem_infos+variacoes"
                logger.info(
                    "🔁 Modo infos_from_variacoes: %d variações + %d infos → %d infos merged",
                    len(variacoes_origem), len(infos_origem), len(infos_to_sync),
                )
            else:
                infos_to_sync = infos_origem
                source_context = "origem_infos"
            
            should_create_fields = bool(infos_to_sync)
            
            # ==================== NOVA LÓGICA DE OPÇÕES CHECKED ====================
            origin_checked_options = None
            origem_product_id = str(
                origem_prod.get("produto_id") or origem_prod.get("id") or ""
            ).strip()

            if has_origin_access and origem_product_id:
                logger.info("📖 Coletando opções checked da ORIGEM (produto %s)...", origem_product_id)

                # PRIORIDADE MÁXIMA: Variações da origem (o que você precisa!)
                if destino_is_infos_model and variacoes_origem:
                    origin_checked_options = domain.extract_checked_options_from_variants(
                        origem_prod,
                        page=page,
                        origin_base=origin_base,
                        cookies_origem=cookies_origem,
                        logger=logger,
                    )
                    if origin_checked_options:
                        logger.info("✅ Usando VARIAÇÕES enriquecidas da origem como fonte de checkboxes")
                    else:
                        logger.warning("⚠️ Enriquecimento de variações retornou vazio")

                # Fallback (caso tenha AdditionalInfos diretas na origem)
                if not origin_checked_options:
                    try:
                        origin_checked_options = destino_api.read_origin_checked_options_playwright(
                            page=page, origin_base=origin_base,
                            product_id=origem_product_id,
                            cookies_origem=cookies_origem, logger=logger,
                        )
                        if not origin_checked_options:
                            origin_checked_options = destino_api.read_origin_checked_options(
                                page=page, origin_base=origin_base,
                                product_id=origem_product_id,
                                cookies_origem=cookies_origem, logger=logger,
                            )
                    except Exception as exc:
                        logger.warning("⚠️ Erro lendo ORIGEM: %s", exc)
            # =====================================================================
            
            logger.info(
                "📋 Infos: %d | create=%s | source=%s | options=%s",
                len(infos_to_sync), should_create_fields, source_context,
                "ORIGEM_HTML" if origin_checked_options else "JSON_FALLBACK",
            )
            
            sync_additional_infos(
                page, pid, infos_to_sync, token, log_entry,
                short_delay=_short_delay, medium_delay=_medium_delay,
                create_missing_fields=should_create_fields,
                source_context=source_context,
                origin_checked_options=origin_checked_options,
            )
            
            sync_variants(
                page, pid, origem_prod, token, log_entry,
                short_delay=_short_delay, medium_delay=_medium_delay,
                infos_already_synced=infos_from_variacoes_mode,
                origin_base=origin_base,
                cookies_origem=cookies_origem,
            )

            # Se coletamos campos/opções via DOM na etapa de variantes, atualizar ORIGEM
            try:
                dom_opts = log_entry.get("variants_options_collected") or {}
                origem_prod_id = str(origem_prod.get("produto_id") or origem_prod.get("id") or "").strip()
                if dom_opts and origem_prod_id and origin_base and cookies_origem:
                    infos_for_origin = []
                    for prop, opts in dom_opts.items():
                        op_list = []
                        for o in opts:
                            if not o or not str(o).strip():
                                continue
                            op_list.append({"nome": str(o).strip(), "valor": "0.00"})
                        if op_list:
                            infos_for_origin.append({"nome": prop, "opcoes": op_list})

                    if infos_for_origin:
                        logger.info("🔁 Atualizando AdditionalInfos NA ORIGEM (produto %s) com %d campos", origem_prod_id, len(infos_for_origin))
                        ok, status, body = destino_api.put_origin_additional_infos(
                            page, origin_base, origem_prod_id, infos_for_origin, cookies_origem, logger
                        )
                        if ok:
                            logger.info("✅ ORIGEM AdditionalInfos atualizadas (status %s)", status)
                            log_entry["origin_additional_infos_update"] = {"status": "ok", "http_status": status}
                        else:
                            logger.warning("⚠️ Falha atualizar ORIGEM AdditionalInfos: status=%s body=%s", status, (body or "")[:300])
                            log_entry["origin_additional_infos_update"] = {"status": "error", "http_status": status, "detail": (body or "")[:400]}
            except Exception as exc:
                logger.warning("Erro atualizando ORIGEM AdditionalInfos: %s", exc)
            
            destino_page.append_encontrado_sincronizado(origem_prod.get("nome") or nome)
            log_entry["status"] = "sucesso"
            _save_log(log_entry)
        
        except Exception as exc:
            logger.error("❌ Erro produto %s: %s", pid, exc, exc_info=True)
            log_entry["status"] = "erro_execucao"
            log_entry["erro"] = str(exc)
            failed_count += 1
            destino_page._append_live_result(destino_page.ENCONTRADOS_PATH, origem_prod.get("nome") or nome)
            _save_log(log_entry)
            continue
        
        processed_count += 1
        # Adicionar informações de opções DOM coletadas, se houver
        extra_lines = []
        try:
            dom_opts = log_entry.get("variants_options_collected") or {}
            # dom_opts: { field_name: [opt1, opt2, ...], ... }
            for field, opts in dom_opts.items():
                if not opts:
                    continue
                clicks = []
                for o in opts[:10]:
                    txt = str(o).replace('"', '\\"')
                    clicks.append(f'page.get_by_text("{txt}", exact=True).click()')
                extra_lines.append(f"{field}: " + " ".join(clicks))
        except Exception:
            extra_lines = []

        if extra_lines:
            logger.info("✅ %s/%s | %s\n%s", processed_count, target_count, nome, "\n".join(extra_lines))
        else:
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