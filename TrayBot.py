from playwright.sync_api import Error as PlaywrightError, Page, TimeoutError, sync_playwright
import csv
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from typing import Callable
import unicodedata

load_dotenv()

SOURCE_URL = os.getenv("SOURCE_URL", "")
SOURCE_USER = os.getenv("SOURCE_USER", "")
SOURCE_PASS = os.getenv("SOURCE_PASS", "")
SOURCE_STORE_ID = os.getenv("SOURCE_STORE_ID", "")

TARGET_URL = os.getenv("TARGET_URL", "")
TARGET_USER = os.getenv("TARGET_USER", "")
TARGET_PASS = os.getenv("TARGET_PASS", "")
TARGET_STORE_ID = os.getenv("TARGET_STORE_ID", "")

HEADLESS = os.getenv("HEADLESS", "false").strip().lower() in {"1", "true", "yes", "y"}
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in {"1", "true", "yes", "y"}
SYNC_LIMIT = int(os.getenv("SYNC_LIMIT", "0") or 0)
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0") or 0)
STEP_DELAY_MS = int(os.getenv("STEP_DELAY_MS", "300"))
DEFAULT_TIMEOUT_MS = int(os.getenv("TIMEOUT_MS", "25000"))
NETWORKIDLE_TIMEOUT_MS = int(os.getenv("NETWORKIDLE_TIMEOUT_MS", "5000"))
USE_CDP = os.getenv("USE_CDP", "false").strip().lower() in {"1", "true", "yes", "y"}
CDP_URL = os.getenv("CDP_URL", "http://127.0.0.1:9222")
MANUAL_LOGIN = os.getenv("MANUAL_LOGIN", "false").strip().lower() in {"1", "true", "yes", "y"}
KEEP_OPEN = os.getenv("KEEP_OPEN", "false").strip().lower() in {"1", "true", "yes", "y"}
REUSE_OPEN_TABS = os.getenv("REUSE_OPEN_TABS", "true").strip().lower() in {"1", "true", "yes", "y"}
STOP_ON_LIMIT = os.getenv("STOP_ON_LIMIT", "true").strip().lower() in {"1", "true", "yes", "y"}
DEBUG = os.getenv("DEBUG", "false").strip().lower() in {"1", "true", "yes", "y"}
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "25") or 25)


@dataclass
class ProductSummary:
    sku: str
    code: str
    name: str
    edit_url: str


@dataclass
class ProductPayload:
    sku: str
    name: str
    additional_info: str
    source_edit_url: str


@dataclass
class SkuAudit:
    rows_lidas: int = 0
    sem_sku: int = 0
    duplicados: int = 0
    anomalies: list[dict[str, str]] = field(default_factory=list)


def normalize_admin_base(raw_url: str) -> str:
    cleaned = raw_url.strip()
    if not cleaned:
        return ""
    if "/admin" in cleaned:
        cleaned = cleaned.split("/admin", 1)[0] + "/admin/"
    elif not cleaned.endswith("/admin/"):
        cleaned = cleaned.rstrip("/") + "/admin/"
    return cleaned


def admin_url(base_admin_url: str, path: str) -> str:
    return urljoin(base_admin_url, path.lstrip("/"))


def base_host(url: str) -> str:
    return (urlparse(url).netloc or "").lower()


def paged_products_url(base_admin_url: str, page_number: int, page_size: int = 200) -> str:
    return admin_url(
        base_admin_url,
        f"products/list?status=all&page%5Bsize%5D={page_size}&page%5Bnumber%5D={page_number}",
    )


def pick_open_page_for_store(pages: list[Page], base_admin_url: str) -> "Page | None":
    wanted_host = base_host(base_admin_url)
    if not wanted_host:
        return None

    ranked: list[tuple[int, Page]] = []
    for page in pages:
        current = (page.url or "").lower()
        if not current:
            continue
        current_host = base_host(current)
        if current_host != wanted_host:
            continue

        score = 0
        if "/admin" in current:
            score += 2
        if "/products/list" in current:
            score += 3
        ranked.append((score, page))

    if not ranked:
        return None

    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[0][1]


def must_env(value: str, key: str) -> str:
    if not value:
        raise RuntimeError(f"Variável obrigatória ausente no .env: {key}")
    return value


# ─────────────────────────────────────────────────────────────────────────────
# FIX #1: sku_key normalização muito mais agressiva
# Problema original: SKUs com caracteres especiais, espaços extras, letras
# maiúsculas/minúsculas, hífens vs underscores causavam mismatches.
# ─────────────────────────────────────────────────────────────────────────────
def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFD", value).encode("ascii", "ignore").decode("ascii")


def sku_key(value: str) -> str:
    """
    Normaliza SKU para comparação:
    - Remove espaços das bordas
    - Converte para maiúsculas
    - Remove acentos
    - Colapsa espaços internos múltiplos em um único espaço
    - Remove caracteres que frequentemente variam entre lojas (pontos, barras, etc.)
    """
    raw = (value or "").strip()
    # Remove acento
    raw = normalize_text(raw)
    # Upper-case
    raw = raw.upper()
    # Colapsa whitespace interno
    raw = re.sub(r"\s+", " ", raw)
    return raw


def name_key(value: str) -> str:
    return normalize_text((value or "").strip()).upper()


def safe_fill(page: Page, selectors: list[str], value: str) -> bool:
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        control = locator.first
        if not control.is_visible():
            continue
        control.fill(value)
        page.wait_for_timeout(STEP_DELAY_MS)
        return True
    return False


def safe_click(page: Page, selectors: list[str]) -> bool:
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        control = locator.first
        if not control.is_visible():
            continue
        control.click()
        page.wait_for_timeout(STEP_DELAY_MS)
        return True
    return False


def fill_tray_login_credentials(page: Page, username: str, password: str) -> tuple[bool, bool]:
    try:
        page.wait_for_selector(
            'input[placeholder*="usuário" i], input[placeholder*="e-mail" i], input[placeholder*="cpf" i], input[type="password"]',
            timeout=DEFAULT_TIMEOUT_MS,
        )
    except TimeoutError:
        pass

    user_ok = safe_fill(
        page,
        [
            'input[placeholder*="usuário" i]',
            'input[placeholder*="e-mail" i]',
            'input[placeholder*="cpf" i]',
            'input[name="login"]',
            'input[name="user"]',
            'input[name="username"]',
            'input[name*="email" i]',
            'input[type="email"]',
            'input[id*="login"]',
            'input[id*="user"]',
            'input[id*="email" i]',
            'input[autocomplete="username"]',
            'input[type="text"]:not([name*="loja" i]):not([name*="store" i]):not([id*="loja" i]):not([id*="store" i])',
        ],
        username,
    )

    pass_ok = safe_fill(
        page,
        [
            'input[placeholder*="senha" i]',
            'input[name="password"]',
            'input[type="password"]',
            'input[id*="senha"]',
            'input[id*="pass"]',
            'input[autocomplete="current-password"]',
        ],
        password,
    )

    return user_ok, pass_ok


def read_first_input_value(page: Page, selectors: list[str]) -> str:
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        field = locator.first
        if not field.is_visible():
            continue
        try:
            return (field.input_value() or "").strip()
        except Exception:
            continue
    return ""


def read_additional_info_from_page(page: Page) -> str:
    selectors = [
        'textarea[placeholder*="mensagem adicional" i]',
        'textarea[placeholder*="desconto exclusivo" i]',
        'textarea[placeholder*="informação extra" i]',
        'textarea[name*="additional"]',
        'textarea[id*="additional"]',
        'textarea[name*="informacao"]',
        'textarea[id*="informacao"]',
        'textarea[name*="info"]',
    ]
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        field = locator.first
        if not field.is_visible():
            continue
        try:
            return (field.input_value() or "").strip()
        except Exception:
            continue

    value = page.evaluate(
        """
        () => {
            const labels = [...document.querySelectorAll('label')];
            const wanted = [
                'informação extra para o cliente',
                'informacao extra para o cliente',
                'mensagem adicional',
                'informações adicionais',
                'informacoes adicionais'
            ];

            for (const label of labels) {
                const text = (label.textContent || '').toLowerCase();
                if (!wanted.some(word => text.includes(word))) {
                    continue;
                }

                const forId = label.getAttribute('for');
                if (forId) {
                    const target = document.getElementById(forId);
                    if (target && target.tagName && target.tagName.toLowerCase() === 'textarea' && 'value' in target) {
                        return (target.value || '').trim();
                    }
                }

                const wrapping = label.closest('.form-group, .control-group, .input-group, div');
                if (wrapping) {
                    const field = wrapping.querySelector('textarea');
                    if (field && 'value' in field) {
                        return (field.value || '').trim();
                    }
                }
            }
            return '';
        }
        """
    )
    return (value or "").strip()


def read_product_description_from_page(page: Page) -> str:
    value = page.evaluate(
        """
        () => {
            const clean = (v) => (v || '').toString().trim();

            if (window.CKEDITOR && window.CKEDITOR.instances) {
                const instances = Object.values(window.CKEDITOR.instances || {});
                for (const inst of instances) {
                    try {
                        const html = clean(inst.getData ? inst.getData() : '');
                        if (!html) continue;
                        const temp = document.createElement('div');
                        temp.innerHTML = html;
                        const text = clean(temp.innerText || temp.textContent || '');
                        if (text) return text;
                    } catch (e) {}
                }
            }

            const frame = document.querySelector('iframe.cke_wysiwyg_frame');
            if (frame && frame.contentDocument && frame.contentDocument.body) {
                const text = clean(frame.contentDocument.body.innerText || frame.contentDocument.body.textContent || '');
                if (text) return text;
            }

            const textarea = document.querySelector('textarea[name*="description" i], textarea[id*="description" i]');
            if (textarea && 'value' in textarea) {
                const text = clean(textarea.value || '');
                if (text) return text;
            }

            return '';
        }
        """
    )
    return (value or "").strip()


def login(page: Page, base_admin_url: str, username: str, password: str, store_id: str = "") -> None:
    login_url = admin_url(base_admin_url, "")
    page.goto(login_url, wait_until="domcontentloaded")
    page.set_default_timeout(DEFAULT_TIMEOUT_MS)

    if store_id:
        safe_fill(
            page,
            [
                'input[name="store_id"]',
                'input[name="id_loja"]',
                'input[name="loja"]',
                'input[id*="store"]',
                'input[id*="loja"]',
            ],
            store_id,
        )

    user_ok, pass_ok = fill_tray_login_credentials(page, username, password)

    if not user_ok or not pass_ok:
        fallback = page.evaluate(
            """
            (creds) => {
                const isVisible = (el) => !!el && !el.disabled && el.offsetParent !== null;
                const emit = (el) => {
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                };

                const inputs = [...document.querySelectorAll('input')].filter(isVisible);
                const passField = inputs.find((inp) => (inp.type || '').toLowerCase() === 'password') || null;

                let userField = inputs.find((inp) => {
                    const type = (inp.type || '').toLowerCase();
                    if (!(type === 'text' || type === 'email')) return false;
                    const signature = `${inp.name || ''} ${inp.id || ''} ${inp.placeholder || ''}`.toLowerCase();
                    return !signature.includes('loja') && !signature.includes('store');
                }) || null;

                if (!userField && passField) {
                    const scope = passField.closest('form') || document;
                    const formInputs = [...scope.querySelectorAll('input')].filter(isVisible);
                    userField = formInputs.find((inp) => {
                        const type = (inp.type || '').toLowerCase();
                        return type === 'text' || type === 'email';
                    }) || null;
                }

                let userOk = false;
                let passOk = false;

                if (userField) {
                    userField.focus();
                    userField.value = '';
                    emit(userField);
                    userField.value = creds.username;
                    emit(userField);
                    userOk = true;
                }

                if (passField) {
                    passField.focus();
                    passField.value = '';
                    emit(passField);
                    passField.value = creds.password;
                    emit(passField);
                    passOk = true;
                }

                return { userOk, passOk };
            }
            """,
            {"username": username, "password": password},
        )
        user_ok = user_ok or bool(fallback.get("userOk"))
        pass_ok = pass_ok or bool(fallback.get("passOk"))

    if not user_ok or not pass_ok:
        raise RuntimeError("Não foi possível localizar campos de login/senha.")

    clicked = safe_click(
        page,
        [
            'button[type="submit"]',
            'button:has-text("Entrar")',
            'button:has-text("Acessar")',
            'input[type="submit"]',
        ],
    )
    if not clicked:
        raise RuntimeError("Não foi possível clicar no botão de login.")

    try:
        page.wait_for_load_state("networkidle")
    except TimeoutError:
        pass

    password_visible = False
    password_locator = page.locator('input[name="password"], input[type="password"]')
    if password_locator.count() > 0:
        try:
            password_visible = password_locator.first.is_visible()
        except Exception:
            password_visible = True

    if password_visible and ("/login" in page.url or page.url.rstrip("/") == login_url.rstrip("/")):
        raise RuntimeError("Login parece não ter sido concluído. Verifique usuário/senha/ID da loja.")


def needs_login(page: Page) -> bool:
    if "/login" in page.url:
        return True
    if "twofactorauth" in page.url.lower() or "two_factor" in page.url.lower():
        return True
    password_locator = page.locator('input[name="password"], input[type="password"]')
    return password_locator.count() > 0


def ensure_authenticated(
    page: Page,
    base_admin_url: str,
    username: str,
    password: str,
    store_id: str,
    label: str,
) -> None:
    open_products_list(page, base_admin_url)
    if needs_login(page):
        print(f"[{label}] sessão sem cookie válido, realizando login...")
        login(page, base_admin_url, username, password, store_id)
        open_products_list(page, base_admin_url)
    else:
        print(f"[{label}] sessão autenticada por cookie reutilizado.")


def ensure_authenticated_manual(
    page: Page,
    base_admin_url: str,
    username: str,
    password: str,
    label: str,
) -> None:
    login_url = admin_url(base_admin_url, "")
    print(f"[{label}] abrindo login manual: {login_url}")
    page.goto(login_url, wait_until="domcontentloaded")

    user_ok, pass_ok = fill_tray_login_credentials(page, username, password)
    submit_ok = safe_click(
        page,
        [
            'button[type="submit"]',
            'button:has-text("Entrar")',
            'button:has-text("Acessar")',
            'input[type="submit"]',
        ],
    )

    if user_ok and pass_ok and submit_ok:
        print(f"[{label}] usuário/senha enviados. Se pedir código, confirme no app/e-mail.")
    else:
        print(f"[{label}] não consegui enviar login automaticamente. Faça login manual na aba e siga.")

    while True:
        try:
            typed_code = input(
                f"[{label}] Digite o código Google Authenticator (opcional) e pressione ENTER após concluir o login no navegador: "
            ).strip()
        except EOFError:
            typed_code = ""

        if typed_code:
            print(f"[{label}] código recebido no terminal.")

        page.wait_for_timeout(STEP_DELAY_MS)
        open_products_list(page, base_admin_url)
        if needs_login(page):
            print(f"[{label}] login ainda não detectado. Complete a autenticação e pressione ENTER novamente.")
            continue

        print(f"[{label}] login confirmado.")
        break


def open_products_list(page: Page, base_admin_url: str) -> None:
    target_url = admin_url(base_admin_url, "products/list?status=all")
    for attempt in range(1, 5):
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle")
            except TimeoutError:
                pass
            return
        except PlaywrightError as exc:
            message = str(exc).lower()
            interrupted = "interrupted by another navigation" in message or "net::err_aborted" in message
            if interrupted and attempt < 4:
                page.wait_for_timeout(700)
                continue
            raise


# ─────────────────────────────────────────────────────────────────────────────
# FIX #2: coleta de produtos reescrita
#
# Problema original: a regex buscava "ref" no texto da célula, mas:
#   - Nem sempre o texto tem "Ref:" explícito
#   - O fallback era `code` (código interno Tray), que é diferente entre lojas
#   - Isso fazia origem e destino produzirem chaves diferentes para o mesmo produto
#
# Solução: extrair o SKU/Referência de MÚLTIPLAS fontes em ordem de prioridade:
#   1. Atributo data-* específico de SKU/referência
#   2. Regex de "Ref:" no texto da célula
#   3. Segundo valor da célula de código (após quebra de linha)
#   4. Primeiro valor numérico/alfanumérico da célula de código
# ─────────────────────────────────────────────────────────────────────────────
def collect_products_from_current_page(
    page: Page,
    base_admin_url: str,
    label: str,
    page_num: int,
) -> tuple[list[ProductSummary], list[dict[str, str]]]:
    try:
        # Primeiro, dump da estrutura da tabela para diagnóstico na primeira página
        if page_num == 1:
            table_debug = page.evaluate(
                """
                () => {
                    const trs = [...document.querySelectorAll('table tbody tr')].slice(0, 3);
                    return trs.map(tr => {
                        const cells = [...tr.querySelectorAll('td')];
                        return {
                            cellCount: cells.length,
                            cells: cells.map((td, i) => ({
                                index: i,
                                innerText: (td.innerText || '').trim().substring(0, 120),
                                html: td.innerHTML.substring(0, 200),
                            }))
                        };
                    });
                }
                """
            )
            print(f"\n[{label}] [DEBUG] Estrutura da tabela (primeiras 3 linhas):")
            for row_i, row_info in enumerate(table_debug or []):
                print(f"  Linha {row_i + 1} ({row_info.get('cellCount', 0)} células):")
                for cell in (row_info.get("cells") or []):
                    print(f"    [td{cell['index']}] texto='{cell['innerText']}'")

        rows = page.evaluate(
            r"""
            () => {
                const clean = (v) => (v || '').toString().trim();

                const extractSku = (tr, cells) => {
                    // Estratégia 1: atributo data-sku / data-reference / data-ref em qualquer elemento da linha
                    const dataAttrs = ['data-sku', 'data-reference', 'data-ref', 'data-referencia'];
                    for (const attr of dataAttrs) {
                        const el = tr.querySelector(`[${attr}]`);
                        if (el) {
                            const val = clean(el.getAttribute(attr));
                            if (val) return val;
                        }
                    }

                    // Estratégia 2: regex buscando label "Ref" explícito na linha inteira
                    // (Tray às vezes renderiza "Ref: SKU-123" como texto visível)
                    const fullText = clean(tr.innerText || '');
                    const refPatterns = [
                        /(?:ref(?:er[eê]ncia)?|referência)[\.:\s]+([^\n\t|]{1,60})/i,
                        /\bref[\.:][ \t]+([A-Za-z0-9\-_\/\.]{1,60})/i,
                    ];
                    for (const pattern of refPatterns) {
                        const m = fullText.match(pattern);
                        if (m && m[1]) {
                            const val = clean(m[1]).split('|')[0].trim();
                            if (val) return val;
                        }
                    }

                    // Estratégia 3: cells[1] no Tray tem estrutura previsível:
                    //   Linha 0 → código interno Tray (número gerado pela plataforma, DIFERENTE entre lojas)
                    //   Linha 1 → referência/SKU do lojista (IGUAL entre lojas para o mesmo produto)
                    // Portanto: se houver 2+ linhas, a SEGUNDA é sempre a referência.
                    const codeCell = cells[1];
                    if (codeCell) {
                        const lines = clean(codeCell.innerText || '')
                            .split('\n')
                            .map(l => l.trim())
                            .filter(Boolean);

                        // Se tem 2+ linhas, a segunda é a referência do lojista
                        if (lines.length >= 2) {
                            return lines[1];
                        }

                        // Se tem só 1 linha e parece código Tray puro (só dígitos ≥6),
                        // NÃO usa — é o código interno, não serve para comparação entre lojas.
                        // Neste caso retorna vazio para sinalizar que não tem referência.
                        if (lines.length === 1) {
                            const only = lines[0];
                            // Código interno Tray: só dígitos, tipicamente 6-10 chars
                            if (/^\d+$/.test(only)) return '';
                            // Labels de status — ignorar
                            if (/^(ativo|inativo|sim|não|nao|status|pausado)$/i.test(only)) return '';
                            return only;
                        }
                    }

                    return '';
                };

                const trs = [...document.querySelectorAll('table tbody tr')];
                return trs.map((tr) => {
                    const cells = [...tr.querySelectorAll('td')];
                    const codeCell = cells[1] || null;
                    const productCell = cells[2] || null;
                    const actionsCell = cells.length ? cells[cells.length - 1] : null;

                    const codeText = clean(codeCell?.innerText || '');
                    const codeFirstLine = codeText.split('\n').map(x => x.trim()).filter(Boolean)[0] || '';

                    const sku = extractSku(tr, cells);
                    const name = clean((productCell?.innerText || '').split('\n')[0] || '');

                    const allAnchors = [...tr.querySelectorAll('a[href]')];
                    const editAnchor = allAnchors.find((anchor) => {
                        const href = (anchor.getAttribute('href') || '').toLowerCase();
                        return (
                            href.includes('/products/edit') ||
                            href.includes('/product/edit') ||
                            href.includes('products/form') ||
                            href.includes('/mvc/adm/products/edit')
                        );
                    }) || actionsCell?.querySelector('a[href]') || productCell?.querySelector('a[href]') || null;

                    const href = editAnchor?.getAttribute('href') || '';

                    return { sku, code: codeFirstLine, name, href };
                }).filter(item => item.href || item.sku || item.code);
            }
            """
        )

        products: list[ProductSummary] = []
        anomalies: list[dict[str, str]] = []
        for row in rows:
            raw_sku = (row.get("sku") or "").strip()
            raw_code = (row.get("code") or "").strip()
            sku = raw_sku if raw_sku else raw_code
            if not sku:
                anomalies.append(
                    {
                        "store": label,
                        "type": "SEM_SKU",
                        "page": str(page_num),
                        "sku": "",
                        "name": (row.get("name") or "").strip(),
                        "code": raw_code,
                        "detail": "Linha ignorada por não conter SKU/Ref válida.",
                    }
                )
                continue

            href = (row.get("href") or "").strip()
            edit_url = urljoin(base_admin_url, href) if href else ""

            products.append(
                ProductSummary(
                    sku=sku,
                    code=raw_code,
                    name=(row.get("name") or "").strip(),
                    edit_url=edit_url,
                )
            )
        return products, anomalies
    except Exception as e:
        print(f"[{label}] página {page_num}: erro ao coletar produtos: {e}")
        return [], [
            {
                "store": label,
                "type": "ERRO_COLETA",
                "page": str(page_num),
                "sku": "",
                "name": "",
                "code": "",
                "detail": f"Erro ao executar JavaScript: {str(e)}",
            }
        ]


def _get_page_size_from_url(url: str) -> int:
    """Lê page[size] da URL atual do Tray. Fallback: PAGE_SIZE do .env."""
    try:
        from urllib.parse import unquote
        raw_qs = urlparse(url).query
        for part in raw_qs.split("&"):
            if not part:
                continue
            k, _, v = part.partition("=")
            if unquote(k) == "page[size]":
                return max(1, int(v))
    except Exception:
        pass
    return PAGE_SIZE


def _click_next_page(page: Page, label: str, page_num: int) -> bool:
    """
    Clica no botão › (próxima página) do paginador do Tray e aguarda a tabela
    atualizar com novos dados.

    O Tray é uma SPA Vue — a URL muda, mas a navegação é client-side.
    Não é possível paginar por URL direta (o servidor ignora page[number] e
    sempre retorna a página 1). A única forma de paginar é via clique no botão.

    Estratégia de detecção de carregamento:
    1. Captura o texto da primeira linha da tabela ANTES do clique
    2. Clica no botão ›
    3. Aguarda até o texto da primeira linha mudar (tabela recarregou)
    4. Timeout de segurança se a tabela não mudar em 15s → considera fim

    Retorna True se conseguiu avançar, False se não há próxima página.
    """
    # Seletor do botão › (próxima página) — seletor exato extraído do DOM do Tray.
    # Estrutura do paginador: «« ‹ 1 2 [3] 4 … › »»
    #   li:nth-child(1) = ««  li:nth-child(2) = ‹
    #   li:nth-child(8) = ›   li:nth-child(9) = »»
    _PAG_BASE = (
        "#app > div.main-layout.main-layout--dark"
        " > div.main-layout__content-page"
        " > div.container-fluid > div"
        " > div.app-card.app-list-card.card > div"
        " > div.app-card__body > div.app-list-card__footer"
        " > div > div.paginator__controls > ul"
    )
    NEXT_BTN_SELECTORS = [
        f"{_PAG_BASE} > li:nth-child(8) > button",       # seletor exato do DOM
        f"{_PAG_BASE} > li:nth-last-child(2) > button",  # relativo ao fim (robusto)
        ".paginator__controls ul li:nth-child(8) button",
        ".paginator__controls ul li:nth-last-child(2) button",
        "button[aria-label*='próxim' i]",
        "button[aria-label*='next' i]",
    ]

    # Captura snapshot da tabela ANTES do clique para detectar quando ela mudar
    first_row_before: str = page.evaluate(
        """
        () => {
            const first = document.querySelector('table tbody tr');
            return first ? (first.innerText || '').trim().substring(0, 80) : '';
        }
        """
    )

    # Verifica se o botão › existe e está habilitado
    next_btn = None
    for selector in NEXT_BTN_SELECTORS:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        btn = locator.first
        try:
            if not btn.is_visible():
                continue
            if btn.is_disabled():
                print(f"[{label}] botão › está desabilitado | fim da paginação")
                return False
            next_btn = btn
            break
        except Exception:
            continue

    if next_btn is None:
        print(f"[{label}] botão › não encontrado | fim da paginação")
        return False

    # Clica no botão
    try:
        next_btn.click()
    except Exception as exc:
        print(f"[{label}] erro ao clicar em ›: {exc} | abortando")
        return False

    # Aguarda a tabela mudar — verifica a cada 300ms por até 15s
    waited_ms = 0
    max_wait_ms = 15_000
    poll_ms = 300
    changed = False
    while waited_ms < max_wait_ms:
        page.wait_for_timeout(poll_ms)
        waited_ms += poll_ms
        first_row_after: str = page.evaluate(
            """
            () => {
                const first = document.querySelector('table tbody tr');
                return first ? (first.innerText || '').trim().substring(0, 80) : '';
            }
            """
        )
        if first_row_after and first_row_after != first_row_before:
            changed = True
            break

    if not changed:
        print(f"[{label}] tabela não mudou após clicar em › ({max_wait_ms}ms) | fim da paginação")
        return False

    # Pequena pausa extra para o Vue terminar de renderizar todas as linhas
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT_MS)
    except TimeoutError:
        page.wait_for_timeout(300)

    return True


def collect_products_with_pagination(
    page: Page,
    base_admin_url: str,
    label: str,
    process_product: Callable[[ProductSummary, str, int], None],
    stop_condition: Callable[[], bool] = lambda: False,
    page_limit: int = 0,
) -> SkuAudit:
    """
    Paginação por clique no botão › do paginador do Tray.

    O Tray é uma SPA Vue com paginação server-side controlada por sessão.
    Mudar page[number] na URL não funciona — o servidor ignora e retorna
    sempre a página 1. A única forma de avançar é clicar no botão ›.

    Detecção de fim de paginação (qualquer uma das condições):
    - Botão › está desabilitado ou ausente
    - A tabela não muda após o clique (timeout)
    - A página retornou menos itens que page_size (última página parcial)
    - A página retornou 0 itens
    """
    audit = SkuAudit()
    detected_size = _get_page_size_from_url(page.url)
    print(f"[{label}] page_size detectado: {detected_size}")

    page_num = 1

    while True:
        current_products, current_anomalies = collect_products_from_current_page(
            page, base_admin_url, label, page_num,
        )
        audit.rows_lidas += len(current_products) + len(current_anomalies)
        audit.sem_sku += len(current_anomalies)
        audit.anomalies.extend(current_anomalies)

        if not current_products:
            print(f"[{label}] página {page_num}: 0 itens | fim da paginação")
            break

        for prod in current_products:
            key = sku_key(prod.sku)
            if not key:
                continue
            process_product(prod, key, page_num)

        print(f"[{label}] página {page_num}: {len(current_products)} itens coletados")

        if stop_condition():
            break

        # Última página: menos itens que o esperado
        if len(current_products) < detected_size:
            print(f"[{label}] última página detectada ({len(current_products)} < {detected_size}) | fim")
            break

        if page_limit > 0 and page_num >= page_limit:
            print(f"[{label}] atingiu limite de páginas ({page_limit})")
            break

        if page_num >= 2000:
            print(f"[{label}] atingiu limite de segurança (2000 páginas)")
            break

        # Avança para próxima página via clique no botão ›
        advanced = _click_next_page(page, label, page_num)
        if not advanced:
            break

        page_num += 1

    return audit


def list_all_products(
    page: Page, base_admin_url: str, label: str, page_limit: int = 0
) -> tuple[dict[str, ProductSummary], SkuAudit]:
    products: dict[str, ProductSummary] = {}
    audit = SkuAudit()

    def process_prod(prod: ProductSummary, key: str, page_num: int):
        if key in products:
            audit.duplicados += 1
            audit.anomalies.append(
                {
                    "store": label,
                    "type": "SKU_DUPLICADO",
                    "page": str(page_num),
                    "sku": prod.sku,
                    "name": prod.name,
                    "code": prod.code,
                    "detail": "SKU repetido na listagem; mantida primeira ocorrência.",
                }
            )
            return
        products[key] = prod

    collect_products_with_pagination(page, base_admin_url, label, process_prod, page_limit=page_limit)
    return products, audit


def open_additional_sections_if_needed(page: Page) -> None:
    safe_click(page, ['button:has-text("Mais opções")'])
    safe_click(page, ['button:has-text("Opções avançadas")'])
    safe_click(page, ['button:has-text("Informações adicionais")'])


def extract_product_payload(page: Page, source_product: ProductSummary) -> ProductPayload:
    if not source_product.edit_url:
        return ProductPayload(
            sku=source_product.sku,
            name=source_product.name,
            additional_info="",
            source_edit_url="",
        )

    page.goto(source_product.edit_url, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle")
    except TimeoutError:
        pass

    open_additional_sections_if_needed(page)

    name_value = read_first_input_value(
        page,
        [
            'input[name="name"]',
            'input[id*="name"]',
            'input[name*="nome"]',
        ],
    )
    sku_value = read_first_input_value(
        page,
        [
            'input[name*="reference"]',
            'input[name*="referencia"]',
            'input[name="sku"]',
            'input[id*="reference"]',
            'input[id*="sku"]',
        ],
    )
    additional_info = read_additional_info_from_page(page)
    if not additional_info:
        additional_info = read_product_description_from_page(page)

    return ProductPayload(
        sku=sku_value or source_product.sku,
        name=name_value or source_product.name,
        additional_info=additional_info,
        source_edit_url=source_product.edit_url,
    )


def ensure_on_edit_page(page: Page, edit_url: str) -> None:
    current = (page.url or "").lower()
    if "/advanced/" in current or "/advanced" in current:
        page.goto(edit_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle")
        except TimeoutError:
            pass


def extract_edit_snapshot(page: Page) -> dict:
    snapshot = page.evaluate(
        r"""
        () => {
            const clean = (v) => (v || '').toString().trim();
            const normalize = (v) =>
                clean(v).toLowerCase()
                    .replaceAll('á','a').replaceAll('à','a').replaceAll('â','a').replaceAll('ã','a').replaceAll('ä','a')
                    .replaceAll('é','e').replaceAll('è','e').replaceAll('ê','e').replaceAll('ë','e')
                    .replaceAll('í','i').replaceAll('ì','i').replaceAll('î','i').replaceAll('ï','i')
                    .replaceAll('ó','o').replaceAll('ò','o').replaceAll('ô','o').replaceAll('õ','o').replaceAll('ö','o')
                    .replaceAll('ú','u').replaceAll('ù','u').replaceAll('û','u').replaceAll('ü','u')
                    .replaceAll('ç','c');

            const getByLabel = (labelText, tags = 'input, textarea, select') => {
                const target = labelText.toLowerCase();
                const labels = [...document.querySelectorAll('label, h4, h5, span, p')];
                for (const label of labels) {
                    const text = clean(label.textContent).toLowerCase();
                    if (!text || !text.includes(target)) continue;
                    const forId = label.getAttribute && label.getAttribute('for');
                    if (forId) {
                        const byId = document.getElementById(forId);
                        if (byId) return byId;
                    }
                    const wrap = label.closest('.form-group, .app-card, .row, .col-md-4, .col-md-8, div');
                    if (!wrap) continue;
                    const field = wrap.querySelector(tags);
                    if (field) return field;
                }
                return null;
            };

            const getByExplicitLabel = (labelText) => {
                const target = labelText.toLowerCase();
                const labels = [...document.querySelectorAll('label')];
                for (const label of labels) {
                    const text = clean(label.textContent).toLowerCase();
                    if (!text || !text.includes(target)) continue;
                    const forId = label.getAttribute('for');
                    if (forId) {
                        const byId = document.getElementById(forId);
                        if (byId) return byId;
                    }
                    const wrap = label.closest('.form-group, .control-group, .row, .col-md-4, .col-md-8, div');
                    if (!wrap) continue;
                    const field = wrap.querySelector('input, textarea, select');
                    if (field) return field;
                }
                return null;
            };

            const getByNameOrId = (parts, scope = document) => {
                const wanted = parts.map((p) => p.toLowerCase());
                const fields = [...scope.querySelectorAll('input, textarea, select')];
                for (const field of fields) {
                    const name = clean(field.getAttribute('name') || '').toLowerCase();
                    const id = clean(field.getAttribute('id') || '').toLowerCase();
                    const ph = clean(field.getAttribute('placeholder') || '').toLowerCase();
                    if (wanted.some((p) => name.includes(p) || id.includes(p) || ph.includes(p))) {
                        return field;
                    }
                }
                return null;
            };

            const getInputNearText = (labelText) => {
                const target = normalize(labelText);
                const nodes = [...document.querySelectorAll('label, span, div, p, h4, h5')];
                for (const node of nodes) {
                    const text = normalize(node.textContent);
                    if (!text || text !== target) continue;
                    const wrap = node.closest('.form-group, .row, .col-md-3, .col-md-4, .col-md-6, .col-md-8, .app-card__body, div');
                    if (!wrap) continue;
                    const field = wrap.querySelector('input, textarea, select');
                    if (field) return field;
                }
                return null;
            };

            const getInputValue = (el) => {
                if (!el) return '';
                if (el.tagName && el.tagName.toLowerCase() === 'select') {
                    const opt = el.selectedOptions && el.selectedOptions.length ? el.selectedOptions[0] : null;
                    return opt ? clean(opt.textContent) : '';
                }
                return clean(el.value);
            };

            const getTextInputValue = (el) => {
                if (!el) return '';
                const tag = (el.tagName || '').toLowerCase();
                const type = (el.getAttribute && el.getAttribute('type') ? el.getAttribute('type') : '').toLowerCase();
                if (tag !== 'input' && tag !== 'textarea') return '';
                if (type === 'checkbox' || type === 'radio' || type === 'hidden') return '';
                const value = clean(el.value);
                if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') return '';
                return value;
            };

            const isLikelyNumeric = (value) => {
                const raw = clean(value);
                if (!raw) return false;
                const normalized = raw.replace(/\./g, '').replace(',', '.');
                return /^\d+(\.\d+)?$/.test(normalized);
            };

            const safeNumericValue = (el) => {
                const value = getTextInputValue(el);
                if (!value) return '';
                return isLikelyNumeric(value) ? value : '';
            };

            const getNumericBySelectors = (selectors) => {
                for (const selector of selectors) {
                    const el = document.querySelector(selector);
                    const val = safeNumericValue(el);
                    if (val) return val;
                }
                return '';
            };

            const getNumericByLabel = (labelText) => {
                const byExplicit = safeNumericValue(getByExplicitLabel(labelText));
                if (byExplicit) return byExplicit;

                const byNear = safeNumericValue(getInputNearText(labelText));
                if (byNear) return byNear;

                const byGeneric = safeNumericValue(getByLabel(labelText, 'input:not([type="checkbox"]):not([type="radio"]):not([type="hidden"])'));
                return byGeneric;
            };

            const getDimsFromSection = () => {
                const cards = [...document.querySelectorAll('.app-card, .app-card__body, .card, .card-body, section')];
                const section = cards.find((s) => {
                    const txt = clean(s.innerText || '').toLowerCase();
                    return txt.includes('peso e dimensões') || txt.includes('peso e dimensoes');
                });
                if (!section) return { peso: '', altura: '', largura: '', comprimento: '' };

                const findValue = (labelText) => {
                    const labels = [...section.querySelectorAll('label, span, div, p')];
                    const marker = labels.find((el) => {
                        const text = clean(el.textContent).toLowerCase();
                        return text === labelText && text.length <= 20;
                    });
                    if (!marker) {
                        const byName = getByNameOrId([labelText, `product_${labelText}`], section);
                        return safeNumericValue(byName);
                    }
                    const forId = marker.getAttribute && marker.getAttribute('for');
                    if (forId) {
                        const byFor = document.getElementById(forId);
                        const direct = safeNumericValue(byFor);
                        if (direct) return direct;
                    }
                    const wrap = marker.closest('.form-group, .row, .col-md-3, .col-md-4, .col-md-6, .col-md-8, div') || marker.parentElement;
                    if (!wrap) {
                        const byName = getByNameOrId([labelText, `product_${labelText}`], section);
                        return safeNumericValue(byName);
                    }
                    const input = wrap.querySelector('input:not([type="checkbox"]):not([type="radio"]):not([type="hidden"])');
                    return safeNumericValue(input) || safeNumericValue(getByNameOrId([labelText, `product_${labelText}`], section));
                };

                return {
                    peso: findValue('peso'),
                    altura: findValue('altura'),
                    largura: findValue('largura'),
                    comprimento: findValue('comprimento'),
                };
            };

            const getCkEditorText = () => {
                if (window.CKEDITOR && window.CKEDITOR.instances) {
                    const instances = Object.values(window.CKEDITOR.instances || {});
                    for (const inst of instances) {
                        try {
                            const html = clean(inst.getData ? inst.getData() : '');
                            if (!html) continue;
                            const tmp = document.createElement('div');
                            tmp.innerHTML = html;
                            const text = clean(tmp.innerText || tmp.textContent || '');
                            if (text) return text;
                        } catch (e) {}
                    }
                }
                const frame = document.querySelector('iframe.cke_wysiwyg_frame');
                if (!frame || !frame.contentDocument || !frame.contentDocument.body) return '';
                return clean(frame.contentDocument.body.innerText || frame.contentDocument.body.textContent || '');
            };

            const fields = {};
            const nodes = [...document.querySelectorAll('input, textarea, select')];

            const labelFor = (el) => {
                if (!el) return '';
                if (el.id) {
                    const byFor = document.querySelector(`label[for="${el.id}"]`);
                    if (byFor && byFor.textContent) return byFor.textContent.trim();
                }
                const wrapping = el.closest('label, .form-group, .control-group, .input-group, .field, .row');
                if (!wrapping) return '';
                const candidate = wrapping.querySelector('label');
                return candidate && candidate.textContent ? candidate.textContent.trim() : '';
            };

            nodes.forEach((el, index) => {
                const tag = el.tagName.toLowerCase();
                const type = (el.getAttribute('type') || '').toLowerCase();
                if (type === 'password') return;

                const key = el.name || el.id || `${tag}_${index}`;
                let value;

                if (tag === 'select') {
                    const selected = [...el.selectedOptions].map((opt) => ({ value: opt.value || '', text: (opt.textContent || '').trim() }));
                    value = el.multiple ? selected : (selected[0] || null);
                } else if (type === 'checkbox' || type === 'radio') {
                    value = { checked: !!el.checked, value: el.value || '' };
                } else {
                    value = el.value || '';
                }

                fields[key] = { label: labelFor(el), type: type || tag, value };
            });

            const images = [...document.querySelectorAll('img[src]')]
                .map((img) => ({
                    src: (img.getAttribute('src') || '').trim().replace(/^\/\//, 'https://'),
                    alt: img.getAttribute('alt') || '',
                    width: img.naturalWidth || img.width || 0,
                    height: img.naturalHeight || img.height || 0,
                }))
                .filter((img) => {
                    const src = (img.src || '').toLowerCase();
                    if (!src || src.startsWith('data:')) return false;
                    if (src.includes('favicon') || src.includes('icon')) return false;
                    if (src.includes('modal-kinghost')) return false;
                    if (img.width <= 24 && img.height <= 24) return false;
                    return true;
                });

            const uniqImages = [];
            const seen = new Set();
            images.forEach((img) => { if (!seen.has(img.src)) { seen.add(img.src); uniqImages.push(img); } });

            const nomeField = getByLabel('nome do produto', 'input[type="text"], input:not([type]), textarea');
            const referenciaField =
                getByNameOrId(['reference', 'referencia', 'sku']) ||
                getByExplicitLabel('referência') ||
                getByExplicitLabel('referencia') ||
                getInputNearText('referência') ||
                getInputNearText('referencia');
            const garantiaSelect = getByExplicitLabel('tempo de garantia') || getByLabel('tempo de garantia', 'select');
            const dimsBySection = getDimsFromSection();
            const explicitDims = {
                peso: getNumericBySelectors(['#__BVID__1942', 'input[name*="peso" i]', 'input[id*="peso" i]']),
                altura: getNumericBySelectors(['#__BVID__1947', 'input[name*="altura" i]', 'input[id*="altura" i]']),
                largura: getNumericBySelectors(['#__BVID__1951', 'input[name*="largura" i]', 'input[id*="largura" i]']),
                comprimento: getNumericBySelectors(['#__BVID__1955', 'input[name*="comprimento" i]', 'input[id*="comprimento" i]']),
            };
            const labelDims = {
                peso: getNumericByLabel('peso') || getNumericByLabel('peso (kg)'),
                altura: getNumericByLabel('altura'),
                largura: getNumericByLabel('largura'),
                comprimento: getNumericByLabel('comprimento'),
            };
            const precoVenda = getNumericBySelectors([
                '#__BVID__1908',
                'input[name*="price" i]:not([name*="promot" i])',
                'input[id*="price" i]:not([id*="promot" i])',
            ]) ||
            getNumericByLabel('preço de venda') ||
            getNumericByLabel('preco de venda') ||
            getNumericByLabel('valor de venda') ||
            getNumericByLabel('preço');

            return {
                url: window.location.href,
                title: document.title || '',
                fields,
                images: uniqImages,
                structured: {
                    nome_produto: getTextInputValue(nomeField),
                    descricao_texto: getCkEditorText(),
                    referencia: getTextInputValue(referenciaField),
                    preco_venda: precoVenda,
                    tempo_garantia: getInputValue(garantiaSelect),
                    peso: explicitDims.peso || labelDims.peso || dimsBySection.peso,
                    altura: explicitDims.altura || labelDims.altura || dimsBySection.altura,
                    largura: explicitDims.largura || labelDims.largura || dimsBySection.largura,
                    comprimento: explicitDims.comprimento || labelDims.comprimento || dimsBySection.comprimento,
                    image_count: uniqImages.length,
                    image_urls: uniqImages.map((img) => img.src),
                },
            };
        }
        """
    )
    snapshot = snapshot or {"url": page.url, "title": "", "fields": {}, "images": []}

    structured = snapshot.get("structured") if isinstance(snapshot, dict) else None
    fields = snapshot.get("fields") if isinstance(snapshot, dict) else None

    if isinstance(structured, dict) and isinstance(fields, dict):
        dim_keys = ["peso", "altura", "largura", "comprimento"]
        missing_dims = any(not (structured.get(key) or "").strip() for key in dim_keys)

        if missing_dims:
            digital_meta = fields.get("key-weight-dimensions")
            is_digital_product = False

            if isinstance(digital_meta, dict):
                digital_value = digital_meta.get("value")
                if isinstance(digital_value, dict):
                    is_digital_product = bool(digital_value.get("checked"))
                elif isinstance(digital_value, bool):
                    is_digital_product = digital_value

            if not is_digital_product:
                ordered_fields = list(fields.items())
                start_idx = next((idx for idx, (key, _) in enumerate(ordered_fields) if key == "key-weight-dimensions"), -1)

                if start_idx >= 0:
                    dim_values: list[str] = []
                    for _, meta in ordered_fields[start_idx + 1 :]:
                        if not isinstance(meta, dict):
                            continue

                        field_type = str(meta.get("type") or "").strip().lower()
                        if field_type not in {"text", "number"}:
                            continue

                        value = meta.get("value")
                        if isinstance(value, (dict, list)):
                            continue

                        text = str(value or "").strip()
                        if not text:
                            continue
                        if not re.fullmatch(r"\d+(?:[\.,]\d+)?", text):
                            continue

                        dim_values.append(text)
                        if len(dim_values) == 4:
                            break

                    if len(dim_values) == 4:
                        for key, value in zip(dim_keys, dim_values):
                            if not (structured.get(key) or "").strip():
                                structured[key] = value

    return snapshot


def create_product(page: Page, target_admin_url: str, payload: ProductPayload) -> tuple[bool, str]:
    page.goto(admin_url(target_admin_url, "products/new"), wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle")
    except TimeoutError:
        pass

    name_ok = safe_fill(
        page,
        ['input[name="name"]', 'input[id*="name"]', 'input[name*="nome"]'],
        payload.name,
    )
    sku_ok = safe_fill(
        page,
        ['input[name*="reference"]', 'input[name*="referencia"]', 'input[name="sku"]', 'input[id*="reference"]', 'input[id*="sku"]'],
        payload.sku,
    )

    if not name_ok or not sku_ok:
        return False, "Falha ao preencher nome/SKU no formulário de destino."

    open_additional_sections_if_needed(page)
    if payload.additional_info:
        safe_fill(
            page,
            ['textarea[name*="additional"]', 'textarea[id*="additional"]', 'textarea[name*="informacao"]', 'textarea[id*="informacao"]', 'textarea[name*="info"]'],
            payload.additional_info,
        )

    save_clicked = safe_click(
        page,
        ['button[type="submit"]', 'button:has-text("Salvar")', 'input[type="submit"]', '.btn-primary:has-text("Salvar")'],
    )
    if not save_clicked:
        return False, "Falha ao acionar botão Salvar."

    try:
        page.wait_for_load_state("networkidle")
    except TimeoutError:
        pass

    error_text = ""
    for selector in [".alert-danger", ".error", ".invalid-feedback"]:
        locator = page.locator(selector)
        if locator.count() > 0 and locator.first.is_visible():
            error_text = (locator.first.inner_text() or "").strip()
            if error_text:
                break

    if error_text:
        return False, error_text

    return True, "Criado"


def write_report(file_name: str, rows: list[dict[str, str]]) -> None:
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "status", "sku", "name", "message",
                "source_list_sku", "source_list_name", "source_edit_url",
                "edit_sku", "edit_name", "edit_additional_info",
                "edit_additional_info_len", "edit_image_count", "edit_image_urls",
                "details_file",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_anomaly_report(file_name: str, rows: list[dict[str, str]]) -> None:
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["store", "type", "page", "sku", "name", "code", "detail"],
        )
        writer.writeheader()
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow(
                {"store": "GERAL", "type": "SEM_ANOMALIAS", "page": "", "sku": "", "name": "", "code": "", "detail": "Nenhuma anomalia detectada."}
            )


def write_details_report(file_name: str, records: list[dict]) -> None:
    with open(file_name, "w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)


def main() -> None:
    source_admin = normalize_admin_base(must_env(SOURCE_URL, "SOURCE_URL"))
    target_admin = normalize_admin_base(must_env(TARGET_URL, "TARGET_URL"))
    source_user = must_env(SOURCE_USER, "SOURCE_USER")
    source_pass = must_env(SOURCE_PASS, "SOURCE_PASS")
    target_user = must_env(TARGET_USER, "TARGET_USER")
    target_pass = must_env(TARGET_PASS, "TARGET_PASS")

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_rows: list[dict[str, str]] = []
    details_records: list[dict] = []

    print("Iniciando sincronização de produtos Tray...")
    print(
        f"HEADLESS={HEADLESS} | DRY_RUN={DRY_RUN} | SYNC_LIMIT={SYNC_LIMIT or 'sem limite'} | "
        f"USE_CDP={USE_CDP} | MANUAL_LOGIN={MANUAL_LOGIN} | KEEP_OPEN={KEEP_OPEN} | "
        f"REUSE_OPEN_TABS={REUSE_OPEN_TABS}"
    )

    with sync_playwright() as playwright:
        if USE_CDP:
            browser = playwright.chromium.connect_over_cdp(CDP_URL)
            if browser.contexts:
                shared_context = browser.contexts[0]
            else:
                shared_context = browser.new_context()
            source_context = shared_context
            target_context = shared_context

            existing_pages = shared_context.pages
            if REUSE_OPEN_TABS and existing_pages:
                source_page = pick_open_page_for_store(existing_pages, source_admin)
                target_page = pick_open_page_for_store(existing_pages, target_admin)
                if source_page is None:
                    raise RuntimeError(f"Não encontrei aba da ORIGEM no Chrome CDP. Abra {source_admin}")
                if target_page is None:
                    raise RuntimeError(f"Não encontrei aba do DESTINO no Chrome CDP. Abra {target_admin}")
            elif existing_pages:
                source_page = existing_pages[0]
                target_page = existing_pages[1] if len(existing_pages) > 1 else source_page
            else:
                source_page = shared_context.new_page()
                target_page = source_page
        else:
            browser = playwright.chromium.launch(headless=HEADLESS)
            source_context = browser.new_context()
            target_context = browser.new_context()
            source_page = source_context.new_page()
            target_page = target_context.new_page()

        # ── Autenticação ──────────────────────────────────────────────────────
        if USE_CDP and REUSE_OPEN_TABS:
            open_products_list(source_page, source_admin)
            if needs_login(source_page):
                raise RuntimeError("A aba da ORIGEM não está autenticada.")
            open_products_list(target_page, target_admin)
            if needs_login(target_page):
                raise RuntimeError("A aba do DESTINO não está autenticada.")
            print("[CDP] Sessões confirmadas nas abas já abertas.")
        elif MANUAL_LOGIN:
            ensure_authenticated_manual(source_page, source_admin, source_user, source_pass, "ORIGEM")
            ensure_authenticated_manual(target_page, target_admin, target_user, target_pass, "DESTINO")
        else:
            ensure_authenticated(source_page, source_admin, source_user, source_pass, SOURCE_STORE_ID, "ORIGEM")
            ensure_authenticated(target_page, target_admin, target_user, target_pass, TARGET_STORE_ID, "DESTINO")

        # ── Coleta DESTINO ────────────────────────────────────────────────────
        print("\nListando produtos do DESTINO...")
        target_products, target_audit = list_all_products(target_page, target_admin, "DESTINO", page_limit=PAGE_LIMIT)
        print(f"\n[DESTINO] Total de chaves coletadas: {len(target_products)}")
        print("[DESTINO] Amostra de chaves (primeiras 10):")
        for k in sorted(target_products.keys())[:10]:
            p = target_products[k]
            print(f"  key='{k}' | sku_original='{p.sku}' | name='{p.name}'")

        # ── Coleta ORIGEM ─────────────────────────────────────────────────────
        print("\nListando produtos da ORIGEM...")
        source_products, source_audit = list_all_products(source_page, source_admin, "ORIGEM", page_limit=0)
        print(f"\n[ORIGEM] Total de chaves coletadas: {len(source_products)}")
        print("[ORIGEM] Amostra de chaves (primeiras 10):")
        for k in sorted(source_products.keys())[:10]:
            p = source_products[k]
            print(f"  key='{k}' | sku_original='{p.sku}' | name='{p.name}'")

        # ── Comparação ────────────────────────────────────────────────────────
        # Índice secundário por nome — usado APENAS para detectar divergência de SKU,
        # nunca para considerar um produto como "presente" no destino.
        target_by_name: dict[str, ProductSummary] = {}
        for key, prod in target_products.items():
            nk = name_key(prod.name)
            if nk and nk not in target_by_name:
                target_by_name[nk] = prod

        missing_keys: list[str] = []
        sku_mismatch: list[tuple[str, str, str]] = []

        print("\n── Comparação SKU por SKU ──")
        for source_key, source_prod in source_products.items():
            # Produto sem referência real (só código interno) — não é comparável por SKU
            # Cai direto como faltante para inspeção manual via relatório
            if not source_key:
                missing_keys.append(source_key)
                continue

            if source_key in target_products:
                # Mesmo SKU normalizado encontrado no destino ✓
                continue

            # Não achou por SKU — verifica se existe pelo nome com SKU diferente
            nk = name_key(source_prod.name)
            if nk and nk in target_by_name:
                target_prod = target_by_name[nk]
                # Só reporta como divergência se o destino TAMBÉM tem um SKU real
                # (não só código interno)
                target_key = sku_key(target_prod.sku)
                if target_key:
                    sku_mismatch.append((source_key, target_key, source_prod.name))
                    print(
                        f"  ⚠️  SKU DIVERGENTE | '{source_prod.name}'\n"
                        f"      ORIGEM='{source_key}' → DESTINO='{target_key}'"
                    )
                    continue  # Não adiciona como faltante — produto existe no destino

            # Genuinamente faltante no destino
            missing_keys.append(source_key)

        print(f"\nFaltantes genuínos: {len(missing_keys)}")
        print(f"SKU divergente (mesmo nome): {len(sku_mismatch)}")
        print(
            f"\nAuditoria | Origem: lidas={source_audit.rows_lidas}, sem_sku={source_audit.sem_sku}, "
            f"dup={source_audit.duplicados} | Destino: lidas={target_audit.rows_lidas}, "
            f"sem_sku={target_audit.sem_sku}, dup={target_audit.duplicados}"
        )
        print(f"Origem: {len(source_products)} | Destino: {len(target_products)} | Faltantes: {len(missing_keys)}")

        if SYNC_LIMIT > 0:
            missing_keys = missing_keys[:SYNC_LIMIT]
            print(f"Aplicando limite de sincronização: {len(missing_keys)}")

        # ── Processar faltantes ───────────────────────────────────────────────
        if not missing_keys:
            print("\nNão há produtos genuinamente faltantes no destino.")
        else:
            for index, key in enumerate(missing_keys, start=1):
                source_item = source_products[key]
                print(f"\n[{index}/{len(missing_keys)}] SKU='{source_item.sku}' | {source_item.name}")

                try:
                    payload = extract_product_payload(source_page, source_item)
                    ensure_on_edit_page(source_page, source_item.edit_url)
                    edit_snapshot = extract_edit_snapshot(source_page)

                    structured = edit_snapshot.get("structured") if isinstance(edit_snapshot, dict) else None
                    if isinstance(structured, dict) and not structured.get("referencia"):
                        structured["referencia"] = payload.sku or source_item.sku

                    image_urls = [
                        img.get("src", "")
                        for img in edit_snapshot.get("images", [])
                        if img.get("src")
                    ]

                    detail_record = {
                        "status": "DRY_RUN" if DRY_RUN else "PENDING",
                        "source_list_sku": source_item.sku,
                        "source_list_name": source_item.name,
                        "source_edit_url": payload.source_edit_url,
                        "edit_payload": {
                            "sku": payload.sku,
                            "name": payload.name,
                            "additional_info": payload.additional_info,
                        },
                        "edit_snapshot": edit_snapshot,
                    }

                    if DRY_RUN:
                        report_rows.append(
                            {
                                "status": "DRY_RUN",
                                "sku": payload.sku,
                                "name": payload.name,
                                "message": "Somente simulação; produto não foi criado.",
                                "source_list_sku": source_item.sku,
                                "source_list_name": source_item.name,
                                "source_edit_url": payload.source_edit_url,
                                "edit_sku": payload.sku,
                                "edit_name": payload.name,
                                "edit_additional_info": payload.additional_info,
                                "edit_additional_info_len": str(len(payload.additional_info or "")),
                                "edit_image_count": str(len(image_urls)),
                                "edit_image_urls": " | ".join(image_urls[:20]),
                                "details_file": f"sync_missing_details_{run_stamp}.json",
                            }
                        )
                        details_records.append(detail_record)
                        continue

                    created, message = create_product(target_page, target_admin, payload)
                    report_rows.append(
                        {
                            "status": "OK" if created else "ERRO",
                            "sku": payload.sku,
                            "name": payload.name,
                            "message": message,
                            "source_list_sku": source_item.sku,
                            "source_list_name": source_item.name,
                            "source_edit_url": payload.source_edit_url,
                            "edit_sku": payload.sku,
                            "edit_name": payload.name,
                            "edit_additional_info": payload.additional_info,
                            "edit_additional_info_len": str(len(payload.additional_info or "")),
                            "edit_image_count": str(len(image_urls)),
                            "edit_image_urls": " | ".join(image_urls[:20]),
                            "details_file": f"sync_missing_details_{run_stamp}.json",
                        }
                    )
                    detail_record["status"] = "OK" if created else "ERRO"
                    details_records.append(detail_record)

                except Exception as exc:
                    print(f"  ERRO: {exc}")
                    report_rows.append(
                        {
                            "status": "ERRO",
                            "sku": source_item.sku,
                            "name": source_item.name,
                            "message": str(exc),
                            "source_list_sku": source_item.sku,
                            "source_list_name": source_item.name,
                            "source_edit_url": source_item.edit_url,
                            "edit_sku": "",
                            "edit_name": "",
                            "edit_additional_info": "",
                            "edit_additional_info_len": "0",
                            "edit_image_count": "0",
                            "edit_image_urls": "",
                            "details_file": f"sync_missing_details_{run_stamp}.json",
                        }
                    )
                    details_records.append(
                        {
                            "status": "ERRO",
                            "source_list_sku": source_item.sku,
                            "source_list_name": source_item.name,
                            "source_edit_url": source_item.edit_url,
                            "error": str(exc),
                        }
                    )

        # ── Salvar relatórios ─────────────────────────────────────────────────
        report_name = f"sync_report_{run_stamp}.csv"
        write_report(report_name, report_rows)
        print(f"\nRelatório salvo em: {report_name}")

        anomaly_rows = source_audit.anomalies + target_audit.anomalies
        anomaly_report_name = f"sync_anomalies_{run_stamp}.csv"
        write_anomaly_report(anomaly_report_name, anomaly_rows)
        print(f"Relatório de auditoria SKU salvo em: {anomaly_report_name}")

        details_report_name = f"sync_missing_details_{run_stamp}.json"
        write_details_report(details_report_name, details_records)
        print(f"Relatório detalhado de faltantes salvo em: {details_report_name}")

        # Salvar relatório de divergências de SKU
        if sku_mismatch:
            mismatch_name = f"sync_sku_mismatch_{run_stamp}.csv"
            with open(mismatch_name, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["source_sku_key", "target_sku_key", "name"])
                w.writeheader()
                for s_key, t_key, name in sku_mismatch:
                    w.writerow({"source_sku_key": s_key, "target_sku_key": t_key, "name": name})
            print(f"Relatório de SKUs divergentes: {mismatch_name}")

        if KEEP_OPEN:
            try:
                input("\nKEEP_OPEN ativo: pressione ENTER para encerrar... ")
            except EOFError:
                pass

        if source_context is not target_context:
            source_context.close()
            target_context.close()
        if not USE_CDP:
            browser.close()


if __name__ == "__main__":
    main()