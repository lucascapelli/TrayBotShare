import time
import json
import random
import logging
import re  # ← ADICIONADO PARA LIMPAR PREÇO
from urllib.parse import urlencode

logger = logging.getLogger("additional_info")

# Ajustes operacionais
REQUEST_TIMEOUT_MS = 10000  # timeout padrão para requests (ms)
FETCH_RETRIES = 2           # tentativas para GETs críticos
CREATE_RETRIES = 2          # tentativas para POSTs de criação
PATCH_TEST_LIMIT = 0        # >0 limita quantos selects serão patchados (útil para testes). 0 = sem limite.
PATCH_LIMIT = 999             # Limite de quantos selects corrigir (para teste rápido)


# ---------------------------------------------------------------------------
# Extração de token (mantido igual)
# ---------------------------------------------------------------------------
def _extract_token(page, base_domain: str, timeout_ms: int = 15000) -> str | None:
    token = None

    try:
        logger.info("Tentando interceptar token via requisição à API...")
        with page.expect_response(
            lambda resp: "additional-info" in resp.request.url and "api" in resp.request.url,
            timeout=timeout_ms,
        ) as response_info:
            page.reload(wait_until="domcontentloaded", timeout=20000)
        response = response_info.value
        token = response.request.headers.get("authorization")
        if token:
            logger.info("Token obtido via interceptação de request")
            return _ensure_bearer(token)
    except Exception as e:
        logger.debug("Interceptação falhou: %s", e)

    try:
        token = page.evaluate("""(() => {
            const keys = ['token','access_token','auth_token','authorization','jwt','bearer','api_token','user_token'];
            for (const k of keys) { const v = localStorage.getItem(k); if (v && v.length > 10) return v; }
            for (let i = 0; i < localStorage.length; i++) {
                const k = localStorage.key(i); const v = localStorage.getItem(k);
                if (v && v.startsWith('eyJ')) return v;
            }
            return null;
        })()""")
        if token:
            logger.info("Token obtido via localStorage")
            return _ensure_bearer(token)
    except Exception as e:
        logger.debug("localStorage falhou: %s", e)

    try:
        token = page.evaluate("""(() => {
            const pairs = document.cookie.split('; ');
            const keys = ['authorization','token','access_token','auth_token'];
            for (const k of keys) {
                const found = pairs.find(row => row.toLowerCase().startsWith(k + '='));
                if (found) return found.split('=').slice(1).join('=');
            }
            return null;
        })()""")
        if token:
            logger.info("Token obtido via cookies")
            return _ensure_bearer(token)
    except Exception as e:
        logger.debug("Cookies falhou: %s", e)

    try:
        token = page.evaluate("""(() => {
            if (window.__NUXT__ && window.__NUXT__.state) {
                const s = JSON.stringify(window.__NUXT__.state);
                const m = s.match(/"(?:token|access_token|authorization)":"([^"]+)"/);
                if (m) return m[1];
            }
            if (window._token) return window._token;
            if (window.apiToken) return window.apiToken;
            if (window.axios && window.axios.defaults && window.axios.defaults.headers) {
                const auth = window.axios.defaults.headers.common && window.axios.defaults.headers.common['Authorization'];
                if (auth) return auth;
            }
            return null;
        })()""")
        if token:
            logger.info("Token obtido via JS globais")
            return _ensure_bearer(token)
    except Exception as e:
        logger.debug("JS globais falhou: %s", e)

    try:
        logger.info("Forçando navegação para capturar token...")
        with page.expect_response(
            lambda resp: "api" in resp.request.url and resp.request.headers.get("authorization"),
            timeout=timeout_ms,
        ) as response_info:
            page.evaluate("""(() => {
                if (window.history) { window.history.pushState({}, '', window.location.pathname); window.dispatchEvent(new PopStateEvent('popstate')); }
            })()""")
            page.wait_for_timeout(3000)
            page.reload(wait_until="domcontentloaded", timeout=15000)
        response = response_info.value
        token = response.request.headers.get("authorization")
        if token:
            logger.info("Token obtido via navegação forçada")
            return _ensure_bearer(token)
    except Exception as e:
        logger.debug("Navegação forçada falhou: %s", e)

    logger.error("Todas as tentativas de obter token falharam")
    return None


def _ensure_bearer(token: str) -> str:
    token = token.strip()
    if not token.lower().startswith("bearer "):
        return f"Bearer {token}"
    return token


# ---------------------------------------------------------------------------
# Buscar itens existentes (list + detalhe) via API (apenas leitura)
# ---------------------------------------------------------------------------
def _fetch_all_items(page, api_url: str, headers: dict) -> list:
    """Lista todos items (paginação) — não traz detalhes das options por item."""
    all_items = []
    page_num = 1

    try:
        url = f"{api_url}?sort=id&page[size]=25&page[number]=1"
        response = page.request.get(url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
        if response.status != 200:
            logger.warning("Falha ao buscar itens: Status %d", response.status)
            return []
        data = response.json()
        total_records = data.get("paging", {}).get("total", 0)
        total_pages = (total_records + 24) // 25
        all_items.extend(data.get("data", []))
        page_num = 2
    except Exception as e:
        logger.error("Erro ao buscar itens: %s", e)
        return []

    while page_num <= total_pages:
        try:
            url = f"{api_url}?sort=id&page[size]=25&page[number]={page_num}"
            response = page.request.get(url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
            if response.status != 200:
                logger.warning("Página %d falhou: status %s", page_num, getattr(response, "status", None))
                break
            items = response.json().get("data", [])
            if not items:
                break
            all_items.extend(items)
            page_num += 1
            time.sleep(random.uniform(0.4, 0.8))
        except Exception as e:
            logger.error("Erro na pagina %d: %s", page_num, e)
            break

    return all_items


def _fetch_full_item(page, api_url: str, headers: dict, item_id, timeout_ms: int = REQUEST_TIMEOUT_MS) -> dict | None:
    """
    Buscar detalhe de um item (inclui options). Com retries e timeout.
    """
    url = f"{api_url}/{item_id}"
    last_exc = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            logger.debug("GET detalhe item id=%s (attempt %d) %s", item_id, attempt, url)
            response = page.request.get(url, headers=headers, timeout=timeout_ms)
            if response.status == 200:
                data = response.json()
                item_data = data.get("data") or data
                # Processar opções para garantir formato completo
                if "options" in item_data or "values" in item_data:
                    opts = item_data.get("options") or item_data.get("values") or []
                    normalized_opts = []
                    for opt in opts:
                        if isinstance(opt, str):
                            normalized_opts.append({
                                "value": opt,
                                "price": "0.00",
                                "order": 0,
                                "add_total": 1
                            })
                        elif isinstance(opt, dict):
                            price = opt.get("price") or opt.get("additional_price") or opt.get("valor") or "0.00"
                            normalized_opts.append({
                                "value": opt.get("value") or opt.get("label") or "",
                                "price": str(price),
                                "order": opt.get("order", 0),
                                "add_total": opt.get("add_total", 1)
                            })
                    item_data["options"] = normalized_opts
                return item_data
            else:
                logger.debug("GET detalhe id=%s retornou status %s", item_id, getattr(response, "status", None))
                return None
        except Exception as e:
            last_exc = e
            logger.debug("Erro no GET detalhe id=%s attempt %d: %s", item_id, attempt, e)
            time.sleep(0.5 * attempt)
    logger.error("Falha definitiva ao obter detalhe do item %s: %s", item_id, last_exc)
    return None


def _build_existing_names(items: list) -> set:
    names = set()
    for item in items:
        name = (item.get("custom_name") or item.get("name") or "").strip().lower()
        if name:
            names.add(name)
    return names


# ---------------------------------------------------------------------------
# Payloads base
# ---------------------------------------------------------------------------
BASE_FIELDS = {
    "active", "add_total", "custom_name", "display_value",
    "max_length", "name", "order", "required", "type", "value",
}


def _build_base_payload(item: dict) -> dict:
    """Só campos base — sem options, price_rules, values."""
    return {k: item[k] for k in BASE_FIELDS if k in item}


# ---------------------------------------------------------------------------
# Estratégias de criação do campo principal (API JSON e fallback PHP form)
# ---------------------------------------------------------------------------
def _try_json_api_raw(page, api_url: str, headers: dict, payload: dict, timeout_ms: int = REQUEST_TIMEOUT_MS):
    last_exc = None
    for attempt in range(1, CREATE_RETRIES + 1):
        try:
            logger.debug("POST %s (attempt %d) payload keys=%s", api_url, attempt, list(payload.keys()))
            response = page.request.post(api_url, data=json.dumps(payload), headers=headers, timeout=timeout_ms)
            return response
        except Exception as e:
            last_exc = e
            logger.debug("Erro _try_json_api_raw attempt %d: %s", attempt, e)
            time.sleep(0.5 * attempt)
    logger.error("Falha ao POSTar para %s: %s", api_url, last_exc)
    return None


def _try_php_form(page, base_url: str, item: dict) -> tuple[bool, int, str]:
    type_map = {"text": "T", "select": "S", "textarea": "A"}
    tipo = type_map.get(item.get("type", "text"), "T")

    form_data = {
        "nome_loja": item.get("custom_name", ""),
        "nome_adm": item.get("name", ""),
        "ativa": item.get("active", "1"),
        "exibir_valor": item.get("display_value", "0"),
        "obrigatorio": item.get("required", "0"),
        "contador": item.get("max_length", "0"),
        "tipo": tipo,
        "valor": item.get("value", "0.00"),
        "add_total": item.get("add_total", "1"),
        "ordem": item.get("order", "0"),
    }

    php_url = f"{base_url}/admin/informacao_produto_executar.php?acao=incluir"

    try:
        response = page.request.post(php_url, form=form_data, timeout=REQUEST_TIMEOUT_MS)
        body = ""
        try:
            body = response.text()[:300]
        except Exception:
            pass

        success = response.status in (200, 201, 302)
        return success, response.status, body
    except Exception as e:
        logger.debug("_try_php_form erro: %s", e)
        return False, 0, str(e)


# ---------------------------------------------------------------------------
# FUNÇÕES PARA LIDAR COM OPÇÕES VIA HTML (Tray) — VERSÃO CORRIGIDA
# ---------------------------------------------------------------------------
def _fetch_options_from_html(page, field_id: int, base_url: str) -> list:
    """
    Raspa a tabela HTML da aba 'opcoes' (nome + preço real).
    Funciona tanto na origem quanto no destino.
    """
    options = []
    url = f"{base_url}/adm/extras/informacao_produto_index.php?id={field_id}&aba=opcoes"
    try:
        logger.debug(f"🔍 Scraping opções HTML → id={field_id}")
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2500)

        rows = page.query_selector_all("table.lista tbody tr, table#tabela-opcoes tbody tr, table tbody tr")

        for i, row in enumerate(rows):
            cells = row.query_selector_all("td")
            if len(cells) < 2:
                continue

            nome = cells[0].inner_text().strip()
            valor_text = cells[1].inner_text().strip()

            if not nome or nome.lower() in {"", "sem imagem", "nenhuma", "sem opção"}:
                continue

            price = "0.00"
            if valor_text:
                clean = re.sub(r'[^\d.,]', '', valor_text).replace(',', '.').strip()
                try:
                    price = f"{float(clean):.2f}"
                except ValueError:
                    pass

            options.append({
                "value": nome,
                "price": price,
                "order": i
            })

        logger.info(f"✅ Raspadas {len(options)} opções reais para id={field_id}")
        return options

    except Exception as e:
        logger.error(f"❌ Erro scraping opções id={field_id}: {e}")
        return []


def _create_option_via_php(page, base_url: str, field_id: int, option_data: dict) -> bool:
    url = f"{base_url}/adm/extras/informacao_produto_index.php"
    params = {
        "id": field_id,
        "aba": "opcoes",
        "acao": "adicionar"
    }
    form_data = {
        "id": str(field_id),
        "id_opcao": "0",
        "exibicao_novo": "1",
        "opcao": option_data.get("value") or option_data.get("label", ""),
        "valor": option_data.get("price", "0.00"),
    }

    logger.debug(f"Criando opção → nome='{form_data['opcao']}' | preço=R${form_data['valor']}")

    try:
        resp = page.request.post(url, params=params, form=form_data, timeout=REQUEST_TIMEOUT_MS)
        success = resp.status in (200, 201, 302)
        if not success:
            body = ""
            try:
                body = resp.text()[:500]
            except Exception:
                pass
            logger.error("Falha ao criar opção. Status: %s, Resposta: %s", resp.status, body)
        return success
    except Exception as e:
        logger.debug("Falha ao criar opção via PHP: %s", e)
        return False


def _normalize_option_key(opt: dict) -> str:
    if not opt:
        return ""
    v = opt.get("value") or opt.get("label") or ""
    return str(v).strip().lower()


def _ensure_options_for_field(page, base_url: str, field_id, desired_options: list) -> tuple[int, int, list]:
    created_total = 0
    skipped = 0
    errors = []

    existing = _fetch_options_from_html(page, field_id, base_url)
    existing_keys = set(_normalize_option_key(o) for o in existing)

    to_create = []
    for opt in desired_options:
        key = _normalize_option_key(opt)
        if not key or key in existing_keys:
            if key:
                skipped += 1
            else:
                to_create.append(opt)
            continue
        to_create.append(opt)

    for opt in to_create:
        success = _create_option_via_php(page, base_url, field_id, opt)
        if success:
            created_total += 1
        else:
            errors.append({"option": opt, "error": "Falha na criação via PHP"})
        time.sleep(random.uniform(0.3, 0.6))

    return created_total, skipped, errors


# ---------------------------------------------------------------------------
# Coleta da ORIGEM — COM SCRAPE HTML FORÇADO
# ---------------------------------------------------------------------------
def collect_all_additional_info(page, storage):
    print("\n" + "=" * 70)
    print("📋 COLETANDO INFORMAÇÕES ADICIONAIS DA ORIGEM")
    print("=" * 70)

    base_url = "https://www.grasiely.com.br/admin/api/additional-info"
    page_url = "https://www.grasiely.com.br/admin/products/additional-info"

    try:
        print("📍 Navegando para página de informações adicionais...")
        page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(random.randint(2000, 3500))
        print("✅ Página carregada\n")
    except Exception as e:
        print(f"❌ Erro ao carregar página: {e}")
        return []

    token = _extract_token(page, "grasiely.com.br")
    if not token:
        print("❌ Não foi possível obter token. Abortando.")
        return []

    print(f"✅ Token: {token[:35]}...")

    headers = {
        "Authorization": token,
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }

    all_data = []
    page_num = 1
    max_pages = 50

    try:
        initial_url = f"{base_url}?sort=id&page[size]=25&page[number]=1"
        print("🔍 Verificando total de registros...")

        response = page.request.get(initial_url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
        if response.status != 200:
            print(f"❌ Erro: Status {response.status}")
            return []

        data = response.json()
        total_records = data.get("paging", {}).get("total", 0)
        total_pages = (total_records + 24) // 25

        print(f"✅ Total: {total_records} registros, {total_pages} páginas")
        print("-" * 70)

        items = data.get("data", [])
        all_data.extend(items)
        print(f"  ✅ Página 1/{total_pages}: +{len(items)} (total: {len(all_data)})")

        page_num = 2
        max_pages = min(max_pages, total_pages)

    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

    while page_num <= max_pages:
        url = f"{base_url}?sort=id&page[size]=25&page[number]={page_num}"
        try:
            response = page.request.get(url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
            if response.status != 200:
                print(f"  ⚠️ Página {page_num} falhou (Status {response.status})")
                break
            data = response.json()
            items = data.get("data", [])
            if not items:
                break
            all_data.extend(items)
            print(f"  ✅ Página {page_num}/{total_pages}: +{len(items)} (total: {len(all_data)})")
            page_num += 1
            time.sleep(random.uniform(0.8, 1.5))
        except Exception as e:
            print(f"  ❌ Página {page_num}: {e}")
            break

    # Verificação via API (mantida como fallback)
    print("-" * 70)
    print("🔎 Verificando selects individualmente para capturar options...")
    for item in all_data:
        if item.get("type") == "select" and not item.get("options"):
            item_id = item.get("id")
            if not item_id:
                continue
            full = _fetch_full_item(page, base_url, headers, item_id)
            if full:
                item_options = full.get("options") or []
                item["options"] = item_options
                if item_options:
                    amostra = item_options[0]
                    print(f"   ✅ API capturou {len(item_options)} options para select id={item_id} (ex: {amostra.get('value')} - R$ {amostra.get('price')})")
                else:
                    print(f"   ✅ Capturadas 0 options para select id={item_id}")
            else:
                print(f"   ⚠️ Não foi possível obter detalhe para select id={item_id}")

    # 🔥 FORÇA SCRAPE HTML NA ORIGEM (isso resolve o bug dos 0.00 e nomes errados)
    print("-" * 70)
    print("🔥 FORÇANDO scrape via HTML das opções no ORIGEM...")
    origin_php_base = "https://www.grasiely.com.br"
    scraped = 0
    for item in all_data:
        if item.get("type") == "select":
            field_id = item.get("id")
            if not field_id:
                continue
            real_options = _fetch_options_from_html(page, field_id, origin_php_base)
            if real_options:
                item["options"] = real_options
                scraped += 1
                ex = real_options[0]
                print(f"   ✅ id={field_id} → {len(real_options)} opções reais (ex: '{ex['value']}' R${ex['price']})")
            else:
                print(f"   ⚠️ id={field_id} → 0 opções (HTML falhou)")

    print(f"✅ Scrape HTML ORIGEM finalizado! {scraped} selects corrigidos.")
    print("-" * 70)

    print(f"🎉 COLETA FINALIZADA! Total: {len(all_data)}")
    print("=" * 70)

    storage.save_many(all_data)
    return all_data


# ---------------------------------------------------------------------------
# Sync para DESTINO (mantido igual, agora usa a nova função de scrape)
# ---------------------------------------------------------------------------
def sync_additional_info_to_destino(page, data_list):
    print("\n" + "=" * 70)
    print(f"🔄 SINCRONIZANDO {len(data_list)} INFORMAÇÕES ADICIONAIS PARA O DESTINO")
    print("=" * 70)

    site_base = "https://www.grasielyatacado.com.br"
    api_url = f"{site_base}/admin/api/additional-info"
    page_url = f"{site_base}/admin/products/additional-info"

    try:
        print("📍 Navegando para página de informações adicionais...")
        page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(random.randint(2000, 3500))
        print("✅ Página carregada")
    except Exception as e:
        print(f"❌ Erro ao carregar página: {e}")
        return

    token = _extract_token(page, "grasielyatacado.com.br")
    if not token:
        print("❌ Não foi possível obter token. Abortando.")
        return

    print(f"✅ Token: {token[:35]}...")

    headers_get = {
        "Authorization": token,
        "Accept": "application/json, text/plain, */*",
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
    }
    headers_post = {**headers_get, "Content-Type": "application/json"}

    print("\n🔍 Buscando itens já cadastrados no DESTINO...")
    existing = _fetch_all_items(page, api_url, headers_get)
    existing_names = _build_existing_names(existing)
    print(f"✅ {len(existing)} itens já existem no DESTINO")

    to_sync = []
    skipped = 0
    for item in data_list:
        name = (item.get("custom_name") or item.get("name") or "").strip().lower()
        if name in existing_names:
            skipped += 1
        else:
            to_sync.append(item)

    print(f"⏭️  {skipped} já existem → pulados")
    print(f"📝 {len(to_sync)} precisam ser cadastrados")

    if not to_sync:
        print("\n✅ Nada novo para criar — verificando selects sem options no destino...")
    else:
        print("-" * 70)

    success = 0
    errors = 0
    error_list = []

    texts_total = sum(1 for i in to_sync if i.get("type") != "select")
    selects_total = sum(1 for i in to_sync if i.get("type") == "select")
    print(f"   📊 {texts_total} text/textarea + {selects_total} select")
    print("-" * 70)

    for idx, item in enumerate(to_sync, 1):
        display_name = item.get("custom_name") or item.get("name", "?")
        tipo = item.get("type", "text")
        print(f"[{idx:03d}/{len(to_sync)}] {display_name} (tipo: {tipo})", end="")

        payload = _build_base_payload(item)
        resp = _try_json_api_raw(page, api_url, headers_post, payload)

        created_id = None
        if resp and resp.status in (200, 201):
            try:
                json_body = resp.json()
                created_id = (json_body.get("data") or {}).get("id") or json_body.get("id")
            except Exception:
                created_id = None

        if resp and resp.status in (200, 201):
            success += 1
            print(f" → ✅ API JSON ({resp.status})", end="")
        else:
            print(f" → API falhou ({getattr(resp, 'status', 0)}), tentando PHP form...", end="")
            ok2, status2, body2 = _try_php_form(page, site_base, item)
            if ok2:
                existing = _fetch_all_items(page, api_url, headers_get)
                name_key = (item.get("custom_name") or item.get("name") or "").strip().lower()
                match = next((x for x in existing if (x.get("custom_name") or x.get("name") or "").strip().lower() == name_key), None)
                if match:
                    created_id = match.get("id")
                success += 1
                print(f" ✅ PHP form ({status2})", end="")
            else:
                errors += 1
                print(f" ❌ Ambos falharam", end="")
                body = ""
                try:
                    body = resp.text()[:150]
                except Exception:
                    pass
                error_list.append({
                    "name": display_name,
                    "type": tipo,
                    "api_status": getattr(resp, "status", 0),
                    "api_body": body,
                    "php_status": status2,
                    "php_body": body2,
                    "payload": payload,
                })

        if created_id and item.get("type") == "select":
            options = item.get("options") or []
            if options:
                print(f" (ex: {options[0].get('value')} - R$ {options[0].get('price')})", end="")
            created_count, skipped_count, opt_errors = _ensure_options_for_field(page, site_base, created_id, options)
            if created_count:
                print(f" +{created_count} options", end="")
            if skipped_count:
                print(f" ({skipped_count} já existentes)", end="")
            if opt_errors:
                errors += len(opt_errors)
                error_list.extend([{"name": display_name, "type": "option_error", "detail": e} for e in opt_errors])

        print("")
        time.sleep(random.uniform(0.8, 1.5))

    # Corrigir selects existentes
    print("-" * 70)
    print("🔁 Corrigindo selects existentes no DESTINO...")
    raw_existing = _fetch_all_items(page, api_url, headers_get)
    selects_destino = [e for e in raw_existing if e.get("type") == "select"]

    origem_map = {
        (i.get("custom_name") or i.get("name") or "").strip().lower(): i
        for i in data_list
    }

    patched = 0
    total_to_process = min(len(selects_destino), PATCH_LIMIT)
    if PATCH_TEST_LIMIT and PATCH_TEST_LIMIT > 0:
        total_to_process = min(total_to_process, PATCH_TEST_LIMIT)

    for i, dest_stub in enumerate(selects_destino[:total_to_process], 1):
        dest_id = dest_stub.get("id")
        name_key = (dest_stub.get("custom_name") or dest_stub.get("name") or "").strip().lower()
        print(f"[PATCH] {i}/{total_to_process} id={dest_id} name='{name_key}'", end=" ")

        dest_full = _fetch_full_item(page, api_url, headers_get, dest_id)
        if not dest_full:
            print("→ falha ao obter detalhe (skip)")
            continue

        origem_item = origem_map.get(name_key)
        if not origem_item:
            print("→ sem origem correspondente (skip)")
            continue

        origem_options = origem_item.get("options") or []
        if not origem_options:
            print("→ origem não tem options (skip)")
            continue

        if origem_options:
            print(f"(ex: {origem_options[0].get('value')} - R$ {origem_options[0].get('price')})", end=" ")

        created_count, skipped_count, opt_errors = _ensure_options_for_field(page, site_base, dest_id, origem_options)
        if created_count:
            print(f"→ +{created_count} criadas", end="")
            patched += created_count
        elif skipped_count:
            print("→ nada a criar (já OK)", end="")
        else:
            print("→ nada criado", end="")

        if opt_errors:
            errors += len(opt_errors)
            error_list.extend([{"name": name_key, "type": "option_error", "detail": e} for e in opt_errors])
            print(f" ({len(opt_errors)} errors)", end="")

        print("")
        time.sleep(random.uniform(0.2, 0.5))

    print("-" * 70)
    print("✅ SINCRONIZAÇÃO FINALIZADA!")
    print(f"   Total origem:     {len(data_list)}")
    print(f"   Já existiam:      {skipped}")
    print(f"   Tentados:         {len(to_sync)}")
    print(f"   Sucessos:         {success}")
    print(f"   Erros:            {errors}")
    print(f"   Options adicionadas/patchadas: {patched}")

    if error_list:
        try:
            with open("produtos/sync_errors.json", "w", encoding="utf-8") as f:
                json.dump(error_list, f, indent=2, ensure_ascii=False)
            print(f"\n   📋 Erros em: produtos/sync_errors.json")
        except Exception:
            pass

        by_type = {}
        for err in error_list:
            t = err.get("type", "?")
            by_type[t] = by_type.get(t, 0) + 1
        print("   📊 Erros por tipo:")
        for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"      {t}: {n}")

    print("=" * 70)


# ---------------------------------------------------------------------------
# Comparação
# ---------------------------------------------------------------------------
def compare_additional_info(origem_data, destino_data):
    destino_names = _build_existing_names(destino_data)
    return [
        item for item in origem_data
        if (item.get("custom_name") or item.get("name") or "").strip().lower() not in destino_names
    ]