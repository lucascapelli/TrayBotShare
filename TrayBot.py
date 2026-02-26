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
USE_CDP = os.getenv("USE_CDP", "false").strip().lower() in {"1", "true", "yes", "y"}
CDP_URL = os.getenv("CDP_URL", "http://127.0.0.1:9222")
MANUAL_LOGIN = os.getenv("MANUAL_LOGIN", "false").strip().lower() in {"1", "true", "yes", "y"}
KEEP_OPEN = os.getenv("KEEP_OPEN", "false").strip().lower() in {"1", "true", "yes", "y"}
REUSE_OPEN_TABS = os.getenv("REUSE_OPEN_TABS", "true").strip().lower() in {"1", "true", "yes", "y"}
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0") or 0)
STEP_DELAY_MS = int(os.getenv("STEP_DELAY_MS", "300"))
DEFAULT_TIMEOUT_MS = int(os.getenv("TIMEOUT_MS", "25000"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "25") or 25)


@dataclass
class ProductSummary:
    sku: str
    code: str
    name: str
    edit_url: str


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


def must_env(value: str, key: str) -> str:
    if not value:
        raise RuntimeError(f"Variável obrigatória ausente no .env: {key}")
    return value


def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFD", value).encode("ascii", "ignore").decode("ascii")


def sku_key(value: str) -> str:
    raw = (value or "").strip()
    raw = normalize_text(raw)
    raw = raw.upper()
    raw = re.sub(r"\s+", " ", raw)
    return raw


def name_key(value: str) -> str:
    return normalize_text((value or "").strip()).upper()


def normalize_name_for_match(name: str) -> str:
    """Remove acentos, pontuação, palavras de 2 letras ou menos e ordena os termos."""
    if not name:
        return ""
    # Remove acentos
    name = unicodedata.normalize('NFD', name)
    name = re.sub(r'[\u0300-\u036f]', '', name)
    name = name.upper()
    # Remove pontuação (mantém letras, números e espaços)
    name = re.sub(r'[^A-Z0-9\s]', '', name)
    # Divide em palavras
    words = name.split()
    # Remove palavras com 2 letras ou menos (artigos, preposições comuns)
    words = [w for w in words if len(w) > 2]
    # Ordena para padronizar
    words.sort()
    return ' '.join(words)


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
    # Força ir para página 1
    goto_products_page(page, base_admin_url, 1)
    if needs_login(page):
        print(f"[{label}] sessão sem cookie válido, realizando login...")
        login(page, base_admin_url, username, password, store_id)
        goto_products_page(page, base_admin_url, 1)
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
        goto_products_page(page, base_admin_url, 1)
        if needs_login(page):
            print(f"[{label}] login ainda não detectado. Complete a autenticação e pressione ENTER novamente.")
            continue

        print(f"[{label}] login confirmado.")
        break


def goto_products_page(page: Page, base_admin_url: str, page_number: int) -> None:
    """Navega para uma página específica da listagem de produtos via URL."""
    target_url = admin_url(
        base_admin_url,
        f"products/list?sort=name&page%5Bsize%5D={PAGE_SIZE}&page%5Bnumber%5D={page_number}"
    )
    for attempt in range(1, 4):
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
            # Aguarda tabela carregar
            page.wait_for_selector('table tbody tr', timeout=10000)
            page.wait_for_timeout(500)  # Pequena pausa para Vue renderizar
            return
        except (PlaywrightError, TimeoutError) as exc:
            if attempt < 3:
                print(f"    Tentativa {attempt} falhou, retry...")
                page.wait_for_timeout(1000)
                continue
            raise


def collect_products_from_current_page(
    page: Page,
    base_admin_url: str,
    label: str,
    page_num: int,
) -> tuple[list[ProductSummary], list[dict[str, str]]]:
    """Extrai produtos da página atual usando os seletores corretos do HTML real."""
    try:
        # Debug só na primeira página
        if page_num == 1:
            print(f"[{label}] [DEBUG] URL atual: {page.url}")

        rows = page.evaluate(
            r"""
            () => {
                const clean = (v) => (v || '').toString().trim();

                const trs = [...document.querySelectorAll('table tbody tr')];
                return trs.map((tr) => {
                    // SKU: <small class="product-code__reference"> Ref. 10.0torn2 </small>
                    const refElement = tr.querySelector('small.product-code__reference');
                    let sku = '';
                    if (refElement) {
                        const text = clean(refElement.textContent);
                        // Remove "Ref." do início
                        sku = clean(text.replace(/^ref[\.:\s]*/i, ''));
                    }

                    // Nome: <a class="product-info__title-link">...</a>
                    const nameElement = tr.querySelector('a.product-info__title-link');
                    const name = nameElement ? clean(nameElement.textContent) : '';

                    // Código interno: <div class="product-code__id-value">6619</div>
                    const codeElement = tr.querySelector('.product-code__id-value');
                    const code = codeElement ? clean(codeElement.textContent) : '';

                    // URL de edição: <a href="/admin/products/6619/edit">
                    const editLink = tr.querySelector('a[href*="/products/"][href*="/edit"]');
                    const href = editLink ? (editLink.getAttribute('href') || '') : '';

                    return { sku, code, name, href };
                }).filter(item => item.name);  // precisa ter nome ao menos
            }
            """
        )

        products: list[ProductSummary] = []
        anomalies: list[dict[str, str]] = []

        for row in rows:
            raw_sku = (row.get("sku") or "").strip()
            raw_code = (row.get("code") or "").strip()
            raw_name = (row.get("name") or "").strip()

            if not raw_name:
                anomalies.append({
                    "store": label,
                    "type": "SEM_NOME",
                    "page": str(page_num),
                    "sku": raw_sku,
                    "name": raw_name,
                    "code": raw_code,
                    "detail": "Linha sem nome válido",
                })
                continue

            href = (row.get("href") or "").strip()
            edit_url = urljoin(base_admin_url, href) if href else ""

            products.append(
                ProductSummary(
                    sku=raw_sku,
                    code=raw_code,
                    name=raw_name,
                    edit_url=edit_url,
                )
            )

        return products, anomalies

    except Exception as e:
        print(f"[{label}] página {page_num}: erro ao coletar: {e}")
        return [], [{
            "store": label,
            "type": "ERRO_COLETA",
            "page": str(page_num),
            "sku": "",
            "name": "",
            "code": "",
            "detail": str(e),
        }]


def collect_products_with_pagination(
    page: Page,
    base_admin_url: str,
    label: str,
    page_limit: int = 0,
) -> tuple[dict[str, ProductSummary], SkuAudit]:
    """Coleta produtos paginando via clique no botão 'Próximo'."""
    products: dict[str, ProductSummary] = {}
    audit = SkuAudit()
    page_num = 1

    print(f"[{label}] Iniciando coleta com {PAGE_SIZE} produtos por página...")

    # Vai para a primeira página
    goto_products_page(page, base_admin_url, page_num)

    # Função auxiliar para obter o SKU do primeiro produto (ou fallback para nome)
    def get_first_product_identifier() -> tuple[str, str]:
        """Retorna (sku, nome) do primeiro produto da tabela."""
        try:
            first_row = page.locator('table tbody tr').first
            if first_row.count() == 0:
                return ("", "")

            # SKU
            ref = first_row.locator('small.product-code__reference').first
            sku = ""
            if ref.count() > 0:
                text = ref.text_content() or ""
                sku = re.sub(r'^ref[\.:\s]*', '', text, flags=re.IGNORECASE).strip()

            # Nome
            name_elem = first_row.locator('a.product-info__title-link').first
            name = name_elem.text_content() or "" if name_elem.count() > 0 else ""

            return (sku, name.strip())
        except:
            return ("", "")

    # Primeira coleta
    current_products, current_anomalies = collect_products_from_current_page(
        page, base_admin_url, label, page_num
    )
    # contabiliza linhas lidas (produtos + anomalias)
    audit.rows_lidas += len(current_products) + len(current_anomalies)
    audit.anomalies.extend(current_anomalies)

    # Processa produtos (aceita produtos sem SKU, usa chave por nome quando faltar SKU)
    for prod in current_products:
        raw_sku = (prod.sku or "").strip()
        raw_name = (prod.name or "").strip()
        if raw_sku:
            key = sku_key(raw_sku)
        else:
            # produto sem SKU: registra e usa chave baseada no nome normalizado
            audit.sem_sku += 1
            key = "NAME:" + normalize_name_for_match(raw_name)

            # adiciona anomalia informativa
            audit.anomalies.append({
                "store": label,
                "type": "SEM_SKU",
                "page": str(page_num),
                "sku": raw_sku,
                "name": raw_name,
                "code": prod.code,
                "detail": "Produto sem SKU; indexado por nome.",
            })

        if not key:
            # chave vazia após normalização -> registrar e pular
            audit.anomalies.append({
                "store": label,
                "type": "SEM_CHAVE",
                "page": str(page_num),
                "sku": raw_sku,
                "name": raw_name,
                "code": prod.code,
                "detail": "Não foi possível gerar chave para o produto.",
            })
            continue

        if key in products:
            audit.duplicados += 1
            audit.anomalies.append({
                "store": label,
                "type": "DUPLICADO",
                "page": str(page_num),
                "sku": raw_sku,
                "name": raw_name,
                "code": prod.code,
                "detail": "Chave duplicada ao inserir.",
            })
            continue

        products[key] = prod

    print(f"[{label}] página {page_num}: {len(current_products)} produtos coletados (total: {len(products)})")

    # Paginação via clique
    while True:
        # Verifica se o botão "Próximo" está habilitado
        next_button = page.locator('button[aria-label="Go to next page"]')
        if next_button.count() == 0:
            print(f"[{label}] botão 'Próximo' não encontrado, fim da paginação")
            break

        # Verifica se o botão está desabilitado (última página)
        is_disabled = next_button.first.get_attribute("disabled") is not None
        if is_disabled:
            print(f"[{label}] botão 'Próximo' desabilitado, fim da paginação")
            break

        # Captura identificador do primeiro produto antes do clique
        sku_before, name_before = get_first_product_identifier()

        # Clica no botão "Próximo"
        next_button.first.click()
        page.wait_for_timeout(STEP_DELAY_MS * 2)  # Aguarda um pouco para a UI reagir

        # Aguarda que o identificador do primeiro produto mude
        try:
            page.wait_for_function(
                """
                ([skuBefore, nameBefore]) => {
                    const firstRow = document.querySelector('table tbody tr');
                    if (!firstRow) return false;

                    // Tenta obter o SKU atual
                    const refElement = firstRow.querySelector('small.product-code__reference');
                    let currentSku = '';
                    if (refElement) {
                        const text = (refElement.textContent || '').trim();
                        currentSku = text.replace(/^ref[\.:\s]*/i, '').trim();
                    }

                    // Se temos SKU antes, comparar SKU
                    if (skuBefore && currentSku) {
                        return currentSku !== skuBefore;
                    }

                    // Fallback para nome
                    const nameElement = firstRow.querySelector('a.product-info__title-link');
                    const currentName = nameElement ? (nameElement.textContent || '').trim() : '';
                    return currentName !== nameBefore;
                }
                """,
                arg=[sku_before, name_before],
                timeout=DEFAULT_TIMEOUT_MS
            )
        except TimeoutError:
            print(f"[{label}] timeout aguardando atualização da página, verificando se é a última...")
            # Verifica se o botão "Próximo" agora está desabilitado
            if next_button.first.get_attribute("disabled") is not None:
                print(f"[{label}] botão 'Próximo' agora desabilitado, fim da paginação")
                break
            else:
                print(f"[{label}] não foi possível detectar mudança, encerrando paginação")
                break

        # Pequena pausa extra para Vue terminar de renderizar
        page.wait_for_timeout(500)

        page_num += 1

        # Coleta produtos da nova página
        current_products, current_anomalies = collect_products_from_current_page(
            page, base_admin_url, label, page_num
        )

        audit.rows_lidas += len(current_products) + len(current_anomalies)
        audit.anomalies.extend(current_anomalies)

        # Processa produtos da página (mesma lógica)
        novos = 0
        for prod in current_products:
            raw_sku = (prod.sku or "").strip()
            raw_name = (prod.name or "").strip()
            if raw_sku:
                key = sku_key(raw_sku)
            else:
                audit.sem_sku += 1
                key = "NAME:" + normalize_name_for_match(raw_name)
                audit.anomalies.append({
                    "store": label,
                    "type": "SEM_SKU",
                    "page": str(page_num),
                    "sku": raw_sku,
                    "name": raw_name,
                    "code": prod.code,
                    "detail": "Produto sem SKU; indexado por nome.",
                })

            if not key:
                audit.anomalies.append({
                    "store": label,
                    "type": "SEM_CHAVE",
                    "page": str(page_num),
                    "sku": raw_sku,
                    "name": raw_name,
                    "code": prod.code,
                    "detail": "Não foi possível gerar chave para o produto.",
                })
                continue

            if key in products:
                audit.duplicados += 1
                audit.anomalies.append({
                    "store": label,
                    "type": "DUPLICADO",
                    "page": str(page_num),
                    "sku": raw_sku,
                    "name": raw_name,
                    "code": prod.code,
                    "detail": "Chave duplicada ao inserir.",
                })
                continue

            products[key] = prod
            novos += 1

        print(f"[{label}] página {page_num}: {novos} novos produtos (total: {len(products)})")

        # Condições de parada adicionais
        if page_limit > 0 and page_num >= page_limit:
            print(f"[{label}] atingiu limite de páginas ({page_limit})")
            break

        if page_num >= 500:
            print(f"[{label}] atingiu limite de segurança (500 páginas)")
            break

    return products, audit


def write_products_csv(file_name: str, products: dict[str, ProductSummary], label: str) -> None:
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["sku_key", "sku_original", "code", "name", "edit_url"],
        )
        writer.writeheader()
        for key, prod in sorted(products.items()):
            writer.writerow({
                "sku_key": key,
                "sku_original": prod.sku,
                "code": prod.code,
                "name": prod.name,
                "edit_url": prod.edit_url,
            })
    print(f"✓ {label}: {file_name}")


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
            writer.writerow({
                "store": "GERAL",
                "type": "SEM_ANOMALIAS",
                "page": "",
                "sku": "",
                "name": "",
                "code": "",
                "detail": "Nenhuma anomalia detectada."
            })
    print(f"✓ Anomalias: {file_name}")


def write_missing_report(file_name: str, missing_keys: list[str], source_products: dict[str, ProductSummary]) -> None:
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["sku_key", "sku_original", "name", "code", "edit_url"],
        )
        writer.writeheader()
        for key in sorted(missing_keys):
            prod = source_products[key]
            writer.writerow({
                "sku_key": key,
                "sku_original": prod.sku,
                "name": prod.name,
                "code": prod.code,
                "edit_url": prod.edit_url,
            })
    print(f"✓ Faltantes: {file_name}")


def write_mismatch_report(file_name: str, mismatches: list[tuple[str, str, str]]) -> None:
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["source_sku_key", "target_sku_key", "name"],
        )
        writer.writeheader()
        for s_key, t_key, name in mismatches:
            writer.writerow({
                "source_sku_key": s_key,
                "target_sku_key": t_key,
                "name": name
            })
    print(f"✓ SKU divergente: {file_name}")


def main() -> None:
    source_admin = normalize_admin_base(must_env(SOURCE_URL, "SOURCE_URL"))
    target_admin = normalize_admin_base(must_env(TARGET_URL, "TARGET_URL"))
    source_user = must_env(SOURCE_USER, "SOURCE_USER")
    source_pass = must_env(SOURCE_PASS, "SOURCE_PASS")
    target_user = must_env(TARGET_USER, "TARGET_USER")
    target_pass = must_env(TARGET_PASS, "TARGET_PASS")

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "=" * 80)
    print("GERADOR DE RELATÓRIOS TRAY - VERSÃO FINAL")
    print("=" * 80)
    print(f"Config: HEADLESS={HEADLESS} | USE_CDP={USE_CDP} | PAGE_SIZE={PAGE_SIZE}")
    print(f"        PAGE_LIMIT={PAGE_LIMIT or 'ilimitado'} | KEEP_OPEN={KEEP_OPEN}")
    print("=" * 80 + "\n")

    with sync_playwright() as playwright:
        if USE_CDP:
            browser = playwright.chromium.connect_over_cdp(CDP_URL)
            context = browser.contexts[0] if browser.contexts else browser.new_context()

            # Cria novas abas para garantir estado limpo
            source_page = context.new_page()
            target_page = context.new_page()
        else:
            browser = playwright.chromium.launch(headless=HEADLESS)
            source_context = browser.new_context()
            target_context = browser.new_context()
            source_page = source_context.new_page()
            target_page = target_context.new_page()

        # Autenticação
        print("→ Autenticando nas lojas...\n")
        if MANUAL_LOGIN:
            ensure_authenticated_manual(source_page, source_admin, source_user, source_pass, "ORIGEM")
            ensure_authenticated_manual(target_page, target_admin, target_user, target_pass, "DESTINO")
        else:
            ensure_authenticated(source_page, source_admin, source_user, source_pass, SOURCE_STORE_ID, "ORIGEM")
            ensure_authenticated(target_page, target_admin, target_user, target_pass, TARGET_STORE_ID, "DESTINO")

        # Coleta DESTINO
        print("\n" + "=" * 80)
        print("COLETANDO PRODUTOS DO DESTINO...")
        print("=" * 80)
        target_products, target_audit = collect_products_with_pagination(
            target_page, target_admin, "DESTINO", page_limit=PAGE_LIMIT
        )
        print(f"\n✓ DESTINO: {len(target_products)} produtos coletados")
        print(f"  - Sem SKU (contabilizados): {target_audit.sem_sku}")
        print(f"  - Duplicados: {target_audit.duplicados}")

        # Coleta ORIGEM
        print("\n" + "=" * 80)
        print("COLETANDO PRODUTOS DA ORIGEM...")
        print("=" * 80)
        source_products, source_audit = collect_products_with_pagination(
            source_page, source_admin, "ORIGEM", page_limit=0
        )
        print(f"\n✓ ORIGEM: {len(source_products)} produtos coletados")
        print(f"  - Sem SKU (contabilizados): {source_audit.sem_sku}")
        print(f"  - Duplicados: {source_audit.duplicados}")

        # ========== COMPARAÇÃO ROBUSTA POR NOME ==========
        print("\n" + "=" * 80)
        print("COMPARANDO PRODUTOS...")
        print("=" * 80)

        # Índice do destino: nome normalizado -> primeiro produto encontrado
        target_by_norm: dict[str, ProductSummary] = {}
        for prod in target_products.values():
            norm = normalize_name_for_match(prod.name)
            if norm and norm not in target_by_norm:
                target_by_norm[norm] = prod

        # debug
        print("\n[DEBUG] Exemplos de chaves normalizadas (DESTINO):")
        for i, (norm, prod) in enumerate(list(target_by_norm.items())[:5]):
            print(f"  {prod.name[:60]}... -> {norm}")

        # Resultados
        exact_sku_match = []      # SKU exato (apenas contagem)
        name_match = []           # Nome normalizado corresponde (não faltante)
        missing_keys = []         # Não encontrado

        for source_key, source_prod in source_products.items():
            # 1. SKU exato (apenas quando a chave é derivada do SKU)
            if source_key in target_products:
                exact_sku_match.append(source_key)
                continue

            # 2. Tenta correspondência por nome normalizado
            source_norm = normalize_name_for_match(source_prod.name)
            if source_norm and source_norm in target_by_norm:
                name_match.append(source_key)
                continue

            # 3. Não encontrado
            missing_keys.append(source_key)

        print(f"\nProdutos ORIGEM: {len(source_products)}")
        print(f"  - SKU exato no destino: {len(exact_sku_match)}")
        print(f"  - Correspondente por nome (normalizado): {len(name_match)}")
        print(f"  - Não encontrados (faltantes): {len(missing_keys)}")
        print(f"Total de anomalias: {len(source_audit.anomalies) + len(target_audit.anomalies)}")
        # ==========================================

        # Salvar relatórios
        print("\n" + "=" * 80)
        print("SALVANDO RELATÓRIOS...")
        print("=" * 80 + "\n")

        write_products_csv(f"source_products_{run_stamp}.csv", source_products, "ORIGEM")
        write_products_csv(f"target_products_{run_stamp}.csv", target_products, "DESTINO")

        if missing_keys:
            write_missing_report(f"missing_in_target_{run_stamp}.csv", missing_keys, source_products)

        anomaly_rows = source_audit.anomalies + target_audit.anomalies
        write_anomaly_report(f"anomalies_{run_stamp}.csv", anomaly_rows)

        print("\n" + "=" * 80)
        print("✓ CONCLUÍDO COM SUCESSO!")
        print("=" * 80 + "\n")

        if KEEP_OPEN:
            try:
                input("Pressione ENTER para encerrar... ")
            except EOFError:
                pass

        if not USE_CDP:
            browser.close()


if __name__ == "__main__":
    main()