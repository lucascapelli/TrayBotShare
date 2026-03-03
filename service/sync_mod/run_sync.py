import json
import logging
import os
import random
import time
from typing import Any, List

from service.auth import authenticate
from service.auth import load_storage_state, _resolve_state_path
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


def _extract_token_from_state_data(state_data: dict) -> str:
    if not isinstance(state_data, dict):
        return ""

    for origin in state_data.get("origins") or []:
        for entry in origin.get("localStorage") or []:
            key = (entry.get("name") or "").lower()
            value = (entry.get("value") or "").strip()
            if key in {"token", "access_token", "auth_token", "authorization", "jwt", "bearer", "api_token"} and len(value) > 10:
                return value if value.lower().startswith("bearer ") else f"Bearer {value}"
    return ""


def _extract_token_from_page(page: Any) -> str:
    try:
        token = page.evaluate(
            """
            () => {
                const keys = ['token','access_token','auth_token','authorization','jwt','bearer','api_token'];
                for (const k of keys) {
                    const v = localStorage.getItem(k);
                    if (v && v.length > 10) return v;
                }
                for (let i = 0; i < localStorage.length; i++) {
                    const k = localStorage.key(i);
                    const v = localStorage.getItem(k);
                    if (v && typeof v === 'string' && v.startsWith('eyJ')) return v;
                }
                return null;
            }
            """
        )
    except Exception:
        token = ""

    token = (token or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return token


def _capture_auth_from_admin_requests(page: Any, base_url: str, timeout_ms: int = 9000) -> str:
    captured = ""

    def _on_request(request):
        nonlocal captured
        if captured:
            return
        try:
            url = request.url or ""
            if "/admin/api/" not in url:
                return
            token = (request.headers.get("authorization") or "").strip()
            if token:
                captured = token if token.lower().startswith("bearer ") else f"Bearer {token}"
        except Exception:
            pass

    page.on("request", _on_request)
    try:
        page.goto(f"{base_url}/admin/products/list?sort=name", wait_until="domcontentloaded", timeout=20000)
        waited = 0
        while not captured and waited < timeout_ms:
            page.wait_for_timeout(300)
            waited += 300
    except Exception:
        pass
    finally:
        page.remove_listener("request", _on_request)

    return captured


def _parse_additional_infos(additional_infos_raw: list) -> list:
    parsed = []
    for info in additional_infos_raw or []:
        if not isinstance(info, dict):
            continue

        options = []
        options_raw = info.get("options")
        if isinstance(options_raw, dict):
            iterable = options_raw.values()
        elif isinstance(options_raw, list):
            iterable = options_raw
        else:
            iterable = []

        for option in iterable:
            if not isinstance(option, dict):
                continue
            option_name = (option.get("name") or option.get("opcao") or "").strip()
            if option_name:
                options.append({
                    "nome": option_name,
                    "valor": str(option.get("value") or option.get("valor") or "0.00"),
                })

        parsed.append({
            "nome": (info.get("name") or info.get("custom_name") or "").strip(),
            "tipo": info.get("type"),
            "opcoes": options,
        })
    return parsed


def _parse_variacoes(variant_raw: list) -> list:
    parsed = []
    for item in variant_raw or []:
        if not isinstance(item, dict):
            continue
        sku_raw = item.get("Sku") or item.get("sku") or []
        sku = []
        for sku_item in sku_raw:
            if not isinstance(sku_item, dict):
                continue
            sku_type = (sku_item.get("type") or "").strip()
            sku_value = (sku_item.get("value") or "").strip()
            if sku_type and sku_value:
                sku.append({"type": sku_type, "value": sku_value})

        parsed.append({
            "id": str(item.get("id") or item.get("variant_id") or ""),
            "sku": sku,
            "preco": item.get("price"),
            "estoque": item.get("stock"),
            "referencia": item.get("reference"),
            "peso": item.get("weight"),
        })
    return parsed


def _map_tray_product_to_sync(data: dict) -> dict:
    produto = {}
    for origem_key, destino_key in config.ORIGEM_TRAY_TO_SYNC_MAP.items():
        if origem_key in data:
            produto[destino_key] = data.get(origem_key)

    produto["produto_id"] = str(data.get("id") or produto.get("produto_id") or "")
    produto["nome"] = produto.get("nome") or data.get("name")
    produto["ativo"] = str(data.get("active", "1")) == "1"
    produto["visivel"] = str(data.get("visible", "1")) == "1"
    produto["notificacao_estoque_baixo"] = str(data.get("minimum_stock_alert", "0")) == "1"
    produto["informacoes_adicionais"] = _parse_additional_infos(data.get("AdditionalInfos") or [])
    produto["variacoes"] = _parse_variacoes(data.get("Variant") or [])

    url_data = data.get("url") if isinstance(data.get("url"), dict) else {}
    metatags = data.get("metatag") if isinstance(data.get("metatag"), list) else []
    seo_title = None
    seo_description = None
    for tag in metatags:
        if not isinstance(tag, dict):
            continue
        if tag.get("type") == "title":
            seo_title = tag.get("content")
        elif tag.get("type") == "description":
            seo_description = tag.get("content")

    produto["seo_preview"] = {
        "link": url_data.get("https") if isinstance(url_data, dict) else None,
        "title": seo_title,
        "description": seo_description,
    }
    return produto


def _load_origem_from_tray_api(
    context: Any,
    cookies_origem: list,
    origem_url: str,
    source_user: str,
    source_pass: str,
) -> List[dict]:
    browser = getattr(context, "browser", None)
    if not browser:
        logger.error("❌ Contexto atual não expõe browser para abrir sessão da ORIGEM")
        return []

    state_path = _resolve_state_path(cookies_origem or [])
    state_data = load_storage_state(state_path)
    if not state_data:
        logger.error("❌ Storage state da ORIGEM não encontrado: %s", state_path)
        return []

    token = (os.getenv("ORIGEM_AUTH_TOKEN", "") or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    if not token:
        token = _extract_token_from_state_data(state_data)

    context_origem = browser.new_context(storage_state=state_data)
    page_origem = context_origem.new_page()

    try:
        bootstrap = f"{config.ORIGEM_TRAY_BASE}/admin/products/list"
        page_origem.goto(bootstrap, wait_until="domcontentloaded", timeout=20000)

        live_token = _extract_token_from_page(page_origem)
        if live_token:
            token = live_token
        if not token:
            token = _capture_auth_from_admin_requests(page_origem, config.ORIGEM_TRAY_BASE)

        headers = {
            "Accept": config.ORIGEM_TRAY_ACCEPT,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": bootstrap,
        }
        if token:
            headers["Authorization"] = token

        produtos_sync = []
        page_num = 1
        max_pages = 12
        reauth_attempted = False

        while page_num <= max_pages:
            list_url = (
                f"{config.ORIGEM_TRAY_BASE}/admin/api/products"
                f"?sort=name&page[size]=25&page[number]={page_num}"
            )
            resp = page_origem.request.get(list_url, headers=headers)
            if resp.status != 200:
                if resp.status == 401:
                    refreshed_token = _extract_token_from_page(page_origem)
                    if not refreshed_token:
                        refreshed_token = _capture_auth_from_admin_requests(page_origem, config.ORIGEM_TRAY_BASE)
                    if refreshed_token and refreshed_token != headers.get("Authorization"):
                        headers["Authorization"] = refreshed_token
                        resp = page_origem.request.get(list_url, headers=headers)

                if resp.status == 401 and not reauth_attempted and source_user and source_pass:
                    reauth_attempted = True
                    logger.info("🔐 ORIGEM retornou 401; tentando reautenticar sessão para coleta online...")
                    login_url = origem_url or f"{config.ORIGEM_TRAY_BASE}/admin/products/list"
                    auth_page = authenticate(context_origem, login_url, source_user, source_pass, cookies_origem or [])
                    if auth_page:
                        page_origem = auth_page
                        refreshed_token = _extract_token_from_page(page_origem)
                        if refreshed_token:
                            headers["Authorization"] = refreshed_token
                        resp = page_origem.request.get(list_url, headers=headers)

                if resp.status == 401 and not source_user:
                    logger.warning("SOURCE_USER/SOURCE_PASS ausentes; reautenticação ORIGEM não pode ser executada automaticamente.")

                logger.warning("GET ORIGEM products pág %d falhou: status %d", page_num, resp.status)
                if resp.status == 401:
                    logger.warning("Token ORIGEM inválido/expirado na coleta online.")
                break

            payload = resp.json()
            items = payload.get("data") or []
            if not items:
                break

            for item in items:
                product_id = str(item.get("id") or "").strip()
                if not product_id:
                    continue

                detail_url = f"{config.ORIGEM_TRAY_BASE}{config.ORIGEM_TRAY_PRODUCT_ENDPOINT.format(product_id=product_id)}"
                detail_resp = page_origem.request.get(detail_url, headers=headers)
                if detail_resp.status != 200:
                    continue

                detail_payload = detail_resp.json()
                data = detail_payload.get("data") if isinstance(detail_payload, dict) else None
                if isinstance(data, dict):
                    produtos_sync.append(_map_tray_product_to_sync(data))

                if getattr(config, "RATE_LIMIT", 0) > 0 and len(produtos_sync) >= max(config.RATE_LIMIT * 3, config.RATE_LIMIT):
                    break

            if getattr(config, "RATE_LIMIT", 0) > 0 and len(produtos_sync) >= max(config.RATE_LIMIT * 3, config.RATE_LIMIT):
                break

            paging = payload.get("paging") or {}
            total = int(paging.get("total") or 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1

        logger.info("🌐 ORIGEM online carregada via Tray API: %d produto(s)", len(produtos_sync))
        return produtos_sync
    except Exception as exc:
        logger.error("❌ Erro ao carregar ORIGEM online: %s", exc)
        return []
    finally:
        try:
            context_origem.close()
        except Exception:
            pass


def _load_origem(
    context: Any,
    cookies_origem: list,
    origem_url: str,
    source_user: str,
    source_pass: str,
) -> List[dict]:
    origem_source = str(getattr(config, "ORIGEM_SOURCE", "file")).lower()

    if origem_source != "tray_api":
        logger.error("❌ ORIGEM_SOURCE=%s não permitido no sync. Este fluxo exige origem online (tray_api).", origem_source)
        return []

    if origem_source == "tray_api":
        produtos_online = _load_origem_from_tray_api(
            context,
            cookies_origem,
            origem_url=origem_url,
            source_user=source_user,
            source_pass=source_pass,
        )
        if produtos_online:
            return produtos_online
        logger.error("❌ Falha na ORIGEM online. Fallback local desabilitado por regra do fluxo.")
        return []

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
    produtos = _load_origem(
        context,
        cookies_origem,
        origem_url=origem_url,
        source_user=source_user,
        source_pass=source_pass,
    )
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

    if getattr(config, "RATE_LIMIT", 0) > 0 and len(produtos) > config.RATE_LIMIT:
        logger.info(
            "⚙️ Aplicando rate limit: processando %d de %d produto(s)",
            config.RATE_LIMIT,
            len(produtos),
        )
        produtos = produtos[: config.RATE_LIMIT]

    _log_section("ETAPA 2: Buscando match no DESTINO")
    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta no contexto.")
        return
    page = pages[0]

    blocked_ids = {str(item) for item in getattr(config, "SKIP_DESTINO_PRODUCT_IDS", set())}
    remaining_products = list(produtos)
    processed_count = 0
    target_count = len(produtos)

    while remaining_products and processed_count < target_count:
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
            break

        pid = match["destino_id"]
        nome = match["destino_name"]
        origem_prod = match["origem_product"]

        remaining_products = [p for p in remaining_products if p is not origem_prod]

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
            continue

        if not token:
            print("❌ Token não capturado — impossível prosseguir com API calls")
            log_entry["status"] = "erro_token"
            _save_log(log_entry)
            continue

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

        print("\n" + "─" * 70)
        print(f"✅ Produto finalizado [{pid}] {nome}")
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

        _save_log(log_entry)
        processed_count += 1

    print("\n" + "═" * 70)
    if processed_count == 0:
        if blocked_ids:
            print(f"❌ Nenhum match válido encontrado (IDs bloqueados: {sorted(blocked_ids)}).")
        else:
            print("❌ Nenhum match encontrado.")
    else:
        print(f"✅ SYNC FINALIZADO — {processed_count} produto(s) processado(s)")

    if config.MODO_TESTE_APENAS_COM_INFOS:
        print("\n   ℹ️ MODO TESTE ativo — setar MODO_TESTE_APENAS_COM_INFOS = False pra rodar todos")
    print("═" * 70)
