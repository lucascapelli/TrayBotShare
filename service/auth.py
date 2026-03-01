# auth.py
# Autenticação refatorada: storage_state, backoff exponencial, limite de tentativas,
# validação de sessão baseada nos cookies reais da Tray (trayadmin, backoffice_session).

import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any, List, Optional

from patchright.sync_api import Page

logger = logging.getLogger("auth")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
MAX_LOGIN_ATTEMPTS = 2
BASE_BACKOFF_SECONDS = 10
CAPTCHA_WAIT_SECONDS = 300

# Cookies que a Tray usa para manter sessão (descobertos via inspeção real)
TRAY_SESSION_COOKIES = {"trayadmin", "backoffice_session"}


# ---------------------------------------------------------------------------
# Storage state (substitui gerenciamento avulso de cookies)
# ---------------------------------------------------------------------------
def _resolve_state_path(cookie_files: List[str]) -> str:
    """Deriva o caminho do storage_state a partir da lista de cookie files."""
    base = cookie_files[0] if cookie_files else "state.json"
    return str(Path(base).with_suffix(".state.json"))


def load_storage_state(state_path: str) -> Optional[dict]:
    """Carrega storage_state do disco se existir e for válido."""
    if not os.path.isfile(state_path):
        return None
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "cookies" in data:
            logger.info("Storage state carregado de %s", state_path)
            return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Falha ao ler storage state (%s): %s", state_path, exc)
    return None


def save_storage_state(context: Any, state_path: str) -> bool:
    """Persiste storage_state completo (cookies + localStorage + etc)."""
    try:
        context.storage_state(path=state_path)
        logger.info("Storage state salvo em %s", state_path)
        return True
    except Exception as exc:
        logger.error("Erro ao salvar storage state: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Validação de sessão
# ---------------------------------------------------------------------------
def _get_tray_session_cookies(context: Any, domain_hint: str = "") -> dict:
    """Retorna dict {nome: valor} dos cookies de sessão Tray presentes no contexto."""
    try:
        all_cookies = context.cookies()
    except Exception:
        return {}
    found = {}
    for c in all_cookies:
        name = c.get("name", "")
        if name.lower() in TRAY_SESSION_COOKIES:
            cookie_domain = c.get("domain", "")
            if not domain_hint or domain_hint in cookie_domain:
                found[name] = c.get("value", "")
    return found


def is_session_valid(page: Page, context: Any, domain_hint: str = "") -> bool:
    """
    Verifica se a sessão está ativa com base em:
    1. Presença dos cookies trayadmin / backoffice_session
    2. URL contém /admin/ sem campos de login visíveis
    3. Ausência de redirecionamento para página de login
    """
    # Checar cookies primeiro (mais barato)
    session_cookies = _get_tray_session_cookies(context, domain_hint)
    has_cookies = bool(session_cookies.get("trayadmin"))

    if not has_cookies:
        logger.debug("Cookie 'trayadmin' ausente — sessão inválida")
        return False

    # Checar estado da página
    try:
        url = page.url.lower()
        if "login" in url or "entrar" in url:
            logger.info("Redirecionado para login — sessão expirada")
            return False
        if "/admin/" in url:
            login_fields = page.locator("#usuario, #senha, input[type='password']").count()
            if login_fields == 0:
                logger.debug("Sessão válida (admin sem campos de login)")
                return True
    except Exception as exc:
        logger.warning("Erro ao validar página: %s", exc)

    return has_cookies


# ---------------------------------------------------------------------------
# Detecção de estado da página
# ---------------------------------------------------------------------------
def _needs_login(page: Page) -> bool:
    try:
        url = page.url.lower()
        if "login" in url or "entrar" in url:
            return True
        if page.locator("#usuario, input[type='email'], input[name='username']").count() > 0:
            return True
        if page.locator("input[type='password']").count() > 0:
            return True
        return False
    except Exception:
        return False


def _has_captcha(page: Page) -> bool:
    try:
        return page.evaluate("""(() => {
            const sels = 'iframe[src*="recaptcha"], iframe[src*="hcaptcha"], '
                + 'iframe[src*="captcha"], iframe[src*="turnstile"], '
                + '.g-recaptcha, .h-captcha, .cf-turnstile, [data-sitekey]';
            for (const el of document.querySelectorAll(sels)) {
                const r = el.getBoundingClientRect();
                const s = window.getComputedStyle(el);
                if (r.width > 30 && r.height > 30
                    && s.display !== 'none' && s.visibility !== 'hidden')
                    return true;
            }
            return false;
        })()""")
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Digitação humanizada (mantida do original)
# ---------------------------------------------------------------------------
def _human_type(page: Page, selector: str, text: str) -> None:
    locator = page.locator(selector)
    locator.click()
    for ch in text:
        try:
            locator.press(ch)
        except Exception:
            try:
                locator.type(ch)
            except Exception:
                pass
        time.sleep(random.uniform(0.07, 0.18))


# ---------------------------------------------------------------------------
# Polling passivo para CAPTCHA (sem tentar resolver)
# ---------------------------------------------------------------------------
def _wait_for_human_resolution(
    page: Page, context: Any, domain_hint: str, max_wait: int = CAPTCHA_WAIT_SECONDS
) -> bool:
    """Aguarda passivamente até o humano resolver CAPTCHA/login no navegador."""
    logger.info("CAPTCHA detectado — aguardando resolução manual (max %ds)", max_wait)
    start = time.time()
    last_log = 0

    while (elapsed := int(time.time() - start)) < max_wait:
        if is_session_valid(page, context, domain_hint):
            logger.info("Sessão detectada após resolução manual (%ds)", elapsed)
            return True

        # Log a cada 30s para não poluir
        if elapsed - last_log >= 30:
            logger.info("Aguardando resolução... (%ds/%ds)", elapsed, max_wait)
            last_log = elapsed

        time.sleep(3)

    logger.warning("Timeout aguardando resolução manual (%ds)", max_wait)
    return False


# ---------------------------------------------------------------------------
# Fluxo de login (única tentativa)
# ---------------------------------------------------------------------------
def _execute_login(
    page: Page,
    context: Any,
    username: str,
    password: str,
    domain_hint: str,
) -> bool:
    """
    Executa uma única tentativa de login. Retorna True se sessão confirmada.
    Não faz retry — o chamador controla tentativas.
    """
    if not _needs_login(page):
        logger.info("Página já autenticada — login desnecessário")
        return True

    if not username or not password:
        logger.error("Credenciais não fornecidas")
        return False

    # Se CAPTCHA já presente antes do submit, aguardar humano
    if _has_captcha(page):
        return _wait_for_human_resolution(page, context, domain_hint)

    # Preencher formulário
    logger.info("Preenchendo formulário de login...")
    try:
        page.wait_for_selector(
            "#usuario, input[type='email'], input[name='username']", timeout=15_000
        )
    except Exception:
        logger.warning("Campo de usuário não encontrado no tempo esperado")

    try:
        if page.locator("#usuario").count() > 0:
            _human_type(page, "#usuario", username)
        else:
            _human_type(page, "input[type='email'], input[name='username']", username)
    except Exception as exc:
        logger.error("Erro ao preencher usuário: %s", exc)
        return False

    time.sleep(random.uniform(0.8, 1.6))

    try:
        _human_type(page, "#senha, input[type='password']", password)
    except Exception as exc:
        logger.error("Erro ao preencher senha: %s", exc)
        return False

    time.sleep(random.uniform(1.2, 2.5))

    # Submeter
    try:
        submit = page.locator("button[type='submit'], input[type='submit'], .btn-login")
        if submit.count() > 0:
            submit.first.click()
        else:
            page.keyboard.press("Enter")
    except Exception:
        page.keyboard.press("Enter")

    logger.info("Formulário submetido — aguardando resposta...")
    page.wait_for_timeout(random.randint(4000, 6500))

    # Pós-submit: CAPTCHA pode ter aparecido
    if _has_captcha(page):
        return _wait_for_human_resolution(page, context, domain_hint)

    # OTP (se existir)
    try:
        if page.locator("#code").count() > 0:
            code = input("Digite o código OTP: ").strip()
            page.locator("#code").fill(code)
            page.locator("#code").press("Enter")
            page.wait_for_timeout(4000)
    except Exception:
        pass

    # Verificação final
    if _needs_login(page):
        logger.warning("Página ainda exibe formulário de login após submit")
        return False

    return True


# ---------------------------------------------------------------------------
# Função pública: authenticate
# ---------------------------------------------------------------------------
def authenticate(
    context: Any,
    url: str,
    username: str,
    password: str,
    cookie_files: List[str],
    captcha_wait: int = CAPTCHA_WAIT_SECONDS,
) -> Optional[Page]:
    """
    Abre página, valida sessão existente ou faz login (máx. 2 tentativas com backoff).

    - Usa storage_state para persistência completa (cookies + storage).
    - Só persiste estado após login confirmado.
    - Nunca entra em loop de retry automático.

    Retorna Page autenticada ou None.
    """
    state_path = _resolve_state_path(cookie_files)

    # Extrair domínio para filtrar cookies corretamente
    from urllib.parse import urlparse
    domain_hint = urlparse(url).hostname or ""

    page = context.new_page()

    # 1) Navegar
    logger.info("Navegando para %s", url)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)
    except Exception:
        try:
            page.goto(url, timeout=90_000)
        except Exception as exc:
            logger.error("Falha ao navegar: %s — %s", type(exc).__name__, exc)
            _safe_close_page(page)
            return None

    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass

    # Log de redirecionamento
    current = page.url
    if current != url:
        logger.info("Redirecionado: %s → %s", url, current)

    # 2) Checar se sessão já é válida (cookies carregados no context)
    if is_session_valid(page, context, domain_hint):
        logger.info("Sessão existente válida — login dispensado")
        return page

    # 3) Tentativas de login com backoff
    for attempt in range(1, MAX_LOGIN_ATTEMPTS + 1):
        if attempt > 1:
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 2))
            logger.info(
                "Aguardando %ds antes da tentativa %d/%d (backoff)",
                backoff, attempt, MAX_LOGIN_ATTEMPTS,
            )
            time.sleep(backoff)

            # Recarregar página antes da nova tentativa
            try:
                page.reload(wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

        logger.info("Tentativa de login %d/%d", attempt, MAX_LOGIN_ATTEMPTS)

        success = _execute_login(page, context, username, password, domain_hint)

        if success and is_session_valid(page, context, domain_hint):
            logger.info("Login confirmado na tentativa %d", attempt)
            save_storage_state(context, state_path)
            return page

        if success and not _needs_login(page):
            # Login aparentemente ok mas cookies não batem — salvar mesmo assim
            logger.warning(
                "Login aparente mas cookies de sessão Tray não detectados. "
                "Salvando estado e prosseguindo."
            )
            save_storage_state(context, state_path)
            return page

        logger.warning("Tentativa %d/%d falhou", attempt, MAX_LOGIN_ATTEMPTS)

    # 4) Esgotou tentativas
    logger.error(
        "Login falhou após %d tentativas. Navegador pausado para análise.",
        MAX_LOGIN_ATTEMPTS,
    )
    _log_debug_cookies(context)
    input("Pressione ENTER para encerrar...")
    _safe_close_page(page)
    return None


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------
def _safe_close_page(page: Page) -> None:
    try:
        page.close()
    except Exception:
        pass


def _log_debug_cookies(context: Any) -> None:
    try:
        cookies = context.cookies()
        logger.debug("Cookies no contexto (%d):", len(cookies))
        for c in cookies:
            name = c.get("name", "?")
            domain = c.get("domain", "?")
            val = c.get("value", "")
            preview = val[:12] + "..." if len(val) > 12 else val
            logger.debug("  %s @ %s = %s", name, domain, preview)
    except Exception:
        pass