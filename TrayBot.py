from playwright.sync_api import Page, TimeoutError, sync_playwright
import csv
import os
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv

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
STEP_DELAY_MS = int(os.getenv("STEP_DELAY_MS", "300"))
DEFAULT_TIMEOUT_MS = int(os.getenv("TIMEOUT_MS", "25000"))


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


def must_env(value: str, key: str) -> str:
    if not value:
        raise RuntimeError(f"Variável obrigatória ausente no .env: {key}")
    return value


def sku_key(value: str) -> str:
    return (value or "").strip().upper()


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
            const wanted = ['informações adicionais', 'informacoes adicionais', 'adicional'];

            for (const label of labels) {
                const text = (label.textContent || '').toLowerCase();
                if (!wanted.some(word => text.includes(word))) {
                    continue;
                }

                const forId = label.getAttribute('for');
                if (forId) {
                    const target = document.getElementById(forId);
                    if (target && 'value' in target) {
                        return (target.value || '').trim();
                    }
                }

                const wrapping = label.closest('.form-group, .control-group, .input-group, div');
                if (wrapping) {
                    const field = wrapping.querySelector('textarea, input[type="text"]');
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

    user_ok = safe_fill(
        page,
        [
            'input[name="login"]',
            'input[name="user"]',
            'input[name="username"]',
            'input[type="email"]',
            'input[id*="login"]',
            'input[id*="user"]',
        ],
        username,
    )
    pass_ok = safe_fill(
        page,
        [
            'input[name="password"]',
            'input[type="password"]',
            'input[id*="senha"]',
            'input[id*="pass"]',
        ],
        password,
    )

    if not user_ok or not pass_ok:
        raise RuntimeError("Não foi possível localizar campos de login/senha. Revise os seletores.")

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

    if page.locator('input[name="password"], input[type="password"]').count() > 0 and "/login" in page.url:
        raise RuntimeError("Login parece não ter sido concluído. Verifique usuário/senha/ID da loja.")


def open_products_list(page: Page, base_admin_url: str) -> None:
    page.goto(admin_url(base_admin_url, "products/list"), wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle")
    except TimeoutError:
        pass


def collect_products_from_current_page(page: Page, base_admin_url: str) -> list[ProductSummary]:
    rows = page.evaluate(
        """
        () => {
            const trs = [...document.querySelectorAll('table tbody tr')];
            return trs.map((tr) => {
                const cells = [...tr.querySelectorAll('td')];
                const codeCell = cells[1] || null;
                const productCell = cells[2] || null;
                const actionsCell = cells.length ? cells[cells.length - 1] : null;

                const codeText = (codeCell?.innerText || '').trim();
                const codeFirstLine = codeText.split('\n').map(x => x.trim()).filter(Boolean)[0] || '';
                const refMatch = codeText.match(/Ref\.\s*([^\n]+)/i);
                const sku = (refMatch?.[1] || '').trim();
                const name = ((productCell?.innerText || '').split('\n')[0] || '').trim();

                const editAnchor = actionsCell?.querySelector(
                    'a[href*="/products/edit"], a[href*="/product/edit"], a[href*="products/form"]'
                );
                const href = editAnchor?.getAttribute('href') || '';

                return {
                    sku,
                    code: codeFirstLine,
                    name,
                    href,
                };
            }).filter(item => item.href || item.sku || item.code);
        }
        """
    )

    products: list[ProductSummary] = []
    for row in rows:
        raw_sku = (row.get("sku") or "").strip()
        raw_code = (row.get("code") or "").strip()
        sku = raw_sku if raw_sku else raw_code
        if not sku:
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
    return products


def get_next_button(page: Page):
    selectors = [
        'a[rel="next"]',
        '.pagination li.next a',
        '.pagination a:has-text("Próxima")',
        '.pagination a:has-text("Próximo")',
        '.pagination a[aria-label*="Próxima"]',
        '.pagination a[aria-label*="Proxima"]',
    ]

    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        button = locator.first
        cls = (button.get_attribute("class") or "").lower()
        aria_disabled = (button.get_attribute("aria-disabled") or "").lower()
        href = (button.get_attribute("href") or "").strip()
        if "disabled" in cls or aria_disabled == "true" or href in {"", "#"}:
            continue
        if button.is_visible():
            return button
    return None


def list_all_products(page: Page, base_admin_url: str, label: str) -> dict[str, ProductSummary]:
    open_products_list(page, base_admin_url)
    products: dict[str, ProductSummary] = {}
    visited_urls: set[str] = set()

    for page_num in range(1, 800):
        current_url = page.url
        if current_url in visited_urls:
            break
        visited_urls.add(current_url)

        current_products = collect_products_from_current_page(page, base_admin_url)
        for prod in current_products:
            key = sku_key(prod.sku)
            if key and key not in products:
                products[key] = prod

        print(f"[{label}] página {page_num}: {len(current_products)} itens | acumulado: {len(products)}")

        next_btn = get_next_button(page)
        if not next_btn:
            break

        next_btn.click()
        try:
            page.wait_for_load_state("networkidle")
        except TimeoutError:
            pass
        page.wait_for_timeout(STEP_DELAY_MS)

    return products


def open_additional_sections_if_needed(page: Page) -> None:
    safe_click(page, ['a:has-text("Mais opções")'])
    safe_click(page, ['button:has-text("Mais opções")'])
    safe_click(page, ['a:has-text("Opções avançadas")'])
    safe_click(page, ['button:has-text("Opções avançadas")'])
    safe_click(page, ['a:has-text("Informações adicionais")'])
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

    return ProductPayload(
        sku=sku_value or source_product.sku,
        name=name_value or source_product.name,
        additional_info=additional_info,
        source_edit_url=source_product.edit_url,
    )


def create_product(page: Page, target_admin_url: str, payload: ProductPayload) -> tuple[bool, str]:
    page.goto(admin_url(target_admin_url, "products/new"), wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle")
    except TimeoutError:
        pass

    name_ok = safe_fill(
        page,
        [
            'input[name="name"]',
            'input[id*="name"]',
            'input[name*="nome"]',
        ],
        payload.name,
    )
    sku_ok = safe_fill(
        page,
        [
            'input[name*="reference"]',
            'input[name*="referencia"]',
            'input[name="sku"]',
            'input[id*="reference"]',
            'input[id*="sku"]',
        ],
        payload.sku,
    )

    if not name_ok or not sku_ok:
        return False, "Falha ao preencher nome/SKU no formulário de destino."

    open_additional_sections_if_needed(page)
    if payload.additional_info:
        safe_fill(
            page,
            [
                'textarea[name*="additional"]',
                'textarea[id*="additional"]',
                'textarea[name*="informacao"]',
                'textarea[id*="informacao"]',
                'textarea[name*="info"]',
            ],
            payload.additional_info,
        )

    save_clicked = safe_click(
        page,
        [
            'button[type="submit"]',
            'button:has-text("Salvar")',
            'input[type="submit"]',
            '.btn-primary:has-text("Salvar")',
        ],
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
            fieldnames=["status", "sku", "name", "message", "source_edit_url"],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    source_admin = normalize_admin_base(must_env(SOURCE_URL, "SOURCE_URL"))
    target_admin = normalize_admin_base(must_env(TARGET_URL, "TARGET_URL"))
    source_user = must_env(SOURCE_USER, "SOURCE_USER")
    source_pass = must_env(SOURCE_PASS, "SOURCE_PASS")
    target_user = must_env(TARGET_USER, "TARGET_USER")
    target_pass = must_env(TARGET_PASS, "TARGET_PASS")

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_rows: list[dict[str, str]] = []

    print("Iniciando sincronização de produtos Tray...")
    print(f"HEADLESS={HEADLESS} | DRY_RUN={DRY_RUN} | SYNC_LIMIT={SYNC_LIMIT or 'sem limite'}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=HEADLESS)
        source_context = browser.new_context()
        target_context = browser.new_context()

        source_page = source_context.new_page()
        target_page = target_context.new_page()

        print("Logando na loja origem...")
        login(source_page, source_admin, source_user, source_pass, SOURCE_STORE_ID)
        print("Logando na loja destino...")
        login(target_page, target_admin, target_user, target_pass, TARGET_STORE_ID)

        print("Listando produtos da origem...")
        source_products = list_all_products(source_page, source_admin, "ORIGEM")
        print("Listando produtos do destino...")
        target_products = list_all_products(target_page, target_admin, "DESTINO")

        missing_keys = [key for key in source_products if key not in target_products]
        print(f"Origem: {len(source_products)} | Destino: {len(target_products)} | Faltantes: {len(missing_keys)}")

        if SYNC_LIMIT > 0:
            missing_keys = missing_keys[:SYNC_LIMIT]
            print(f"Aplicando limite de sincronização: {len(missing_keys)}")

        if not missing_keys:
            print("Não há produtos faltantes por SKU/Ref.")
        else:
            for index, key in enumerate(missing_keys, start=1):
                source_item = source_products[key]
                print(f"[{index}/{len(missing_keys)}] SKU {source_item.sku} - {source_item.name}")

                try:
                    payload = extract_product_payload(source_page, source_item)

                    if DRY_RUN:
                        report_rows.append(
                            {
                                "status": "DRY_RUN",
                                "sku": payload.sku,
                                "name": payload.name,
                                "message": "Somente simulação; produto não foi criado.",
                                "source_edit_url": payload.source_edit_url,
                            }
                        )
                        continue

                    created, message = create_product(target_page, target_admin, payload)
                    report_rows.append(
                        {
                            "status": "OK" if created else "ERRO",
                            "sku": payload.sku,
                            "name": payload.name,
                            "message": message,
                            "source_edit_url": payload.source_edit_url,
                        }
                    )
                except Exception as exc:
                    report_rows.append(
                        {
                            "status": "ERRO",
                            "sku": source_item.sku,
                            "name": source_item.name,
                            "message": str(exc),
                            "source_edit_url": source_item.edit_url,
                        }
                    )

        report_name = f"sync_report_{run_stamp}.csv"
        write_report(report_name, report_rows)
        print(f"Relatório salvo em: {report_name}")

        source_context.close()
        target_context.close()
        browser.close()


if __name__ == "__main__":
    main()