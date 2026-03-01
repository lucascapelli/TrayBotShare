# service/additional_info.py
import time
import json
import random
import logging
from urllib.parse import urlencode

logger = logging.getLogger("additional_info")


# ---------------------------------------------------------------------------
# Extração de token
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
# Buscar itens existentes
# ---------------------------------------------------------------------------
def _fetch_all_items(page, api_url: str, headers: dict) -> list:
    all_items = []
    page_num = 1

    try:
        url = f"{api_url}?sort=id&page[size]=25&page[number]=1"
        response = page.request.get(url, headers=headers)
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
            response = page.request.get(url, headers=headers)
            if response.status != 200:
                break
            items = response.json().get("data", [])
            if not items:
                break
            all_items.extend(items)
            page_num += 1
            time.sleep(random.uniform(0.4, 0.8))
        except Exception:
            break

    return all_items


def _build_existing_names(items: list) -> set:
    names = set()
    for item in items:
        name = (item.get("custom_name") or item.get("name") or "").strip().lower()
        if name:
            names.add(name)
    return names


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------

# Campos base que funcionam na API JSON (iguais entre text e select)
BASE_FIELDS = {
    "active", "add_total", "custom_name", "display_value",
    "max_length", "name", "order", "required", "type", "value",
}


def _build_base_payload(item: dict) -> dict:
    """Só campos base — sem options, price_rules, values."""
    return {k: item[k] for k in BASE_FIELDS if k in item}


# ---------------------------------------------------------------------------
# Estratégias de criação
# ---------------------------------------------------------------------------

def _try_json_api(page, api_url: str, headers: dict, payload: dict) -> tuple[bool, int, str]:
    """Tenta criar via JSON API. Retorna (sucesso, status, body)."""
    try:
        response = page.request.post(
            api_url,
            data=json.dumps(payload),
            headers=headers,
        )
        body = ""
        try:
            body = response.text()[:300]
        except Exception:
            pass
        return response.status in (200, 201), response.status, body
    except Exception as e:
        return False, 0, str(e)


def _try_php_form(page, base_url: str, item: dict) -> tuple[bool, int, str]:
    """
    Tenta criar via form POST no endpoint PHP real do painel Tray.
    Baseado no form capturado: informacao_produto_executar.php?acao=incluir

    Mapeamento de campos:
        JSON API         →  Form PHP
        custom_name      →  nome_loja
        name             →  nome_adm
        active           →  ativa
        display_value    →  exibir_valor
        required         →  obrigatorio
        max_length       →  contador
        type             →  tipo (T=text, S=select, A=textarea)
        value            →  valor
    """
    # Mapear tipo
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
        response = page.request.post(
            php_url,
            form=form_data,
        )
        body = ""
        try:
            body = response.text()[:300]
        except Exception:
            pass

        # PHP forms normalmente retornam 200 ou 302 (redirect) em caso de sucesso
        success = response.status in (200, 201, 302)
        return success, response.status, body
    except Exception as e:
        return False, 0, str(e)


# ---------------------------------------------------------------------------
# Coleta da ORIGEM
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

        response = page.request.get(initial_url, headers=headers)
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
            response = page.request.get(url, headers=headers)
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

    print("-" * 70)
    print(f"🎉 COLETA FINALIZADA! Total: {len(all_data)}")
    print("=" * 70)

    storage.save_many(all_data)
    return all_data


# ---------------------------------------------------------------------------
# Sync para DESTINO
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

    # ===== BUSCAR O QUE JÁ EXISTE =====
    print("\n🔍 Buscando itens já cadastrados no DESTINO...")
    existing = _fetch_all_items(page, api_url, headers_get)
    existing_names = _build_existing_names(existing)
    print(f"✅ {len(existing)} itens já existem no DESTINO")

    # ===== FILTRAR =====
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
        print("\n✅ Tudo sincronizado!")
        print("=" * 70)
        return

    # ===== ENVIAR =====
    success = 0
    errors = 0
    error_list = []

    # Separar por tipo para estatísticas
    texts_total = sum(1 for i in to_sync if i.get("type") != "select")
    selects_total = sum(1 for i in to_sync if i.get("type") == "select")
    print(f"   📊 {texts_total} text/textarea + {selects_total} select")
    print("-" * 70)

    for idx, item in enumerate(to_sync, 1):
        display_name = item.get("custom_name") or item.get("name", "?")
        tipo = item.get("type", "text")
        print(f"[{idx:03d}/{len(to_sync)}] {display_name} (tipo: {tipo})", end="")

        # Estratégia 1: JSON API com só campos base (funciona pra text, testar pra select)
        payload = _build_base_payload(item)
        ok, status, body = _try_json_api(page, api_url, headers_post, payload)

        if ok:
            success += 1
            print(f" → ✅ API JSON ({status})")
        else:
            # Estratégia 2: PHP form (endpoint nativo do painel)
            print(f" → API falhou ({status}), tentando PHP form...", end="")
            ok2, status2, body2 = _try_php_form(page, site_base, item)

            if ok2:
                success += 1
                print(f" ✅ PHP form ({status2})")
            else:
                errors += 1
                print(f" ❌ Ambos falharam")
                print(f"       API: {status} {body[:150]}")
                print(f"       PHP: {status2} {body2[:150]}")
                error_list.append({
                    "name": display_name,
                    "type": tipo,
                    "api_status": status,
                    "api_body": body,
                    "php_status": status2,
                    "php_body": body2,
                    "payload": payload,
                })

        time.sleep(random.uniform(0.8, 1.5))

    # ===== RESUMO =====
    print("-" * 70)
    print("✅ SINCRONIZAÇÃO FINALIZADA!")
    print(f"   Total origem:     {len(data_list)}")
    print(f"   Já existiam:      {skipped}")
    print(f"   Tentados:         {len(to_sync)}")
    print(f"   Sucessos:         {success}")
    print(f"   Erros:            {errors}")

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