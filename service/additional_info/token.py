from .config import logger


def _ensure_bearer(token: str) -> str:
    token = token.strip()
    if not token.lower().startswith("bearer "):
        return f"Bearer {token}"
    return token


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
