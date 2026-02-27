# service/scraper.py

import re
from urllib.parse import urljoin
from patchright.sync_api import Page, TimeoutError
import random
import time

# =========================
# 1) COLETA (TELA ABERTA NA PÁGINA DE EDIÇÃO)
# =========================
def collect_product_data(page: Page, produto_id: str) -> dict:
    """
    Assume: já estamos na página de edição do produto.
    produto_id: id numérico extraído da URL (ex: "3307")
    """
    product = {"produto_id": produto_id}

    def log(field, value):
        print(f"[COLETA] {field}: {value}")

    # Referência textual
    try:
        ref_loc = page.locator(r"text=/Ref\.\s*(.+)/").first
        ref_text = ref_loc.inner_text(timeout=5000).strip()
        product["referencia"] = ref_text.replace("Ref.", "").strip()
    except:
        product["referencia"] = None
    log("referencia", product["referencia"])

    # Espera tela de edição
    try:
        page.wait_for_selector(
            "div.product-info-detail:has(h4:has-text('Nome do produto')) input[type='text']",
            timeout=15000
        )
    except TimeoutError:
        pass

    # NOME
    try:
        name_input = page.locator(
            "div.product-info-detail:has(h4:has-text('Nome do produto')) input[type='text']"
        )
        product["nome"] = name_input.input_value(timeout=5000)
    except:
        product["nome"] = None
    log("nome", product["nome"])

    # IMAGEM
    try:
        img = page.get_by_role("img").first
        product["imagem_url"] = img.get_attribute("src", timeout=5000)
    except:
        product["imagem_url"] = None
    log("imagem_url", product["imagem_url"])

    # CATEGORIA
    try:
        categoria_btn = page.get_by_role("button", name=re.compile(r".+"))
        product["categoria"] = categoria_btn.first.inner_text(timeout=5000).strip()
    except:
        product["categoria"] = None
    log("categoria", product["categoria"])

    # DESCRIÇÃO (iframe editor1)
    try:
        iframe = page.frame_locator("iframe[title='Editor de Rich Text, editor1']")
        product["descricao"] = iframe.locator("body").inner_text(timeout=8000)
    except:
        product["descricao"] = None
    log("descricao", product["descricao"])

    # SEO preview
    try:
        seo = page.locator(".product-seo > .product-seo__preview")
        product["seo_preview"] = seo.inner_text(timeout=5000)
    except:
        product["seo_preview"] = None
    log("seo_preview", product["seo_preview"])

    # PREÇO
    try:
        product["preco"] = (
            page.get_by_role("group", name="Preço de venda")
            .get_by_placeholder("0,00")
            .input_value(timeout=5000)
        )
    except:
        product["preco"] = None
    log("preco", product["preco"])

    # ESTOQUE / ESTOQUE MÍNIMO
    def safe_group_value(group_name, placeholder="0"):
        try:
            return page.get_by_role("group", name=group_name).get_by_placeholder(placeholder).input_value(timeout=5000)
        except:
            return None

    product["estoque"] = safe_group_value("Estoque", "0")
    log("estoque", product["estoque"])

    product["estoque_minimo"] = safe_group_value("Estoque mínimo", "0")
    log("estoque_minimo", product["estoque_minimo"])

    # CHECKBOX NOTIFICAÇÃO
    try:
        label = page.locator("label:has-text('Notificação de estoque baixo')")
        checkbox_id = label.get_attribute("for", timeout=5000)
        if checkbox_id:
            product["notificacao_estoque_baixo"] = page.locator(f"#{checkbox_id}").is_checked(timeout=5000)
        else:
            product["notificacao_estoque_baixo"] = False
    except:
        product["notificacao_estoque_baixo"] = False
    log("notificacao_estoque_baixo", product["notificacao_estoque_baixo"])

    # REFERÊNCIA PERSONALIZADA
    try:
        product["referencia_personalizada"] = page.get_by_role("textbox", name="Ex: REF-").input_value(timeout=5000)
    except:
        product["referencia_personalizada"] = None
    log("referencia_personalizada", product["referencia_personalizada"])

    # DIMENSÕES
    product["peso"] = safe_group_value("Peso", "0")
    log("peso", product["peso"])

    product["altura"] = safe_group_value("Altura", "0")
    log("altura", product["altura"])

    product["largura"] = safe_group_value("Largura", "0")
    log("largura", product["largura"])

    product["comprimento"] = safe_group_value("Comprimento", "0")
    log("comprimento", product["comprimento"])

    # GARANTIA
    try:
        garantia_group = page.get_by_role("group", name="Tempo de garantia")
        product["tempo_garantia"] = garantia_group.inner_text(timeout=5000)
    except:
        product["tempo_garantia"] = None
    log("tempo_garantia", product["tempo_garantia"])

    try:
        product["tempo_garantia_personalizado"] = page.get_by_role("textbox", name="Descreva o tempo de garantia").input_value(timeout=5000)
    except:
        product["tempo_garantia_personalizado"] = None
    log("tempo_garantia_personalizado", product["tempo_garantia_personalizado"])

    # ITENS INCLUSOS / MENSAGEM ADICIONAL
    try:
        itens_boxes = page.get_by_role("textbox", name="Ex: 1 adesivo")
        if itens_boxes.count() >= 1:
            product["itens_inclusos"] = itens_boxes.nth(0).input_value(timeout=5000)
        if itens_boxes.count() >= 2:
            product["mensagem_adicional"] = itens_boxes.nth(1).input_value(timeout=5000)
    except:
        product["itens_inclusos"] = None
        product["mensagem_adicional"] = None
    log("itens_inclusos", product["itens_inclusos"])
    log("mensagem_adicional", product["mensagem_adicional"])

    return product


# =========================
# 2) COLETA TODOS OS EDIT_URLS (PAGINAÇÃO COMPLETA PRIMEIRO)
# =========================
def collect_all_edit_urls(page: Page, base_list_url: str) -> list[tuple[str, str]]:
    all_edit_urls = []
    seen_ids = set()
    current_page = 1

    while True:
        print(f"[PAGINAÇÃO] Processando página {current_page}...")

        # Locator mais específico: só links na tabela de produtos (ajuste class se precisar)
        anchors = page.locator("table tbody tr a[href*='/admin/products/']")  # Ou ".table-products tbody tr a" se tiver class
        total_anchors = anchors.count()
        hrefs = []
        for i in range(total_anchors):
            try:
                href = anchors.nth(i).get_attribute("href", timeout=5000)
                if href:
                    hrefs.append(href)
            except:
                continue

        # Print debug: primeiros 3 hrefs (comente depois)
        print(f"[DEBUG] Primeiros hrefs encontrados: {hrefs[:3]}")

        for href in hrefs:
            m = re.search(r"/admin/products/(\d+)(?:/edit)?", href)
            if m:
                produto_id = m.group(1)
                if produto_id in seen_ids:
                    continue
                seen_ids.add(produto_id)
                full_url = urljoin(base_list_url, f"/admin/products/{produto_id}/edit")
                all_edit_urls.append((produto_id, full_url))

        print(f"[PAGINAÇÃO] Encontrados {len(hrefs)} links válidos na página {current_page}. Total únicos até agora: {len(all_edit_urls)}")

        # Próxima página
        next_button = page.get_by_role("menuitem", name="Go to next page")
        if next_button.count() == 0 or not next_button.is_enabled():
            print("[PAGINAÇÃO] Sem próxima página. Coleta de URLs completa.")
            break

        next_button.click()
        try:
            page.wait_for_load_state("domcontentloaded", timeout=20000)
            # time.sleep(random.uniform(1.0, 2.5))  # Delay humano (ative se der bloqueio)
        except:
            break

        current_page += 1

    return all_edit_urls


# =========================
# 3) PROCESSA TODOS OS PRODUTOS (SEM VOLTA PRA LISTAGEM)
# =========================
def process_all_products(page: Page, edit_urls: list[tuple[str, str]], storage) -> list[dict]:
    products = []

    for idx, (produto_id, edit_url) in enumerate(edit_urls, 1):
        print(f"[PROCESSO] {idx}/{len(edit_urls)} - Indo para edição: {produto_id} -> {edit_url}")

        try:
            page.goto(edit_url)
            page.wait_for_load_state("domcontentloaded", timeout=20000)
            # time.sleep(random.uniform(0.5, 1.5))  # Delay humano opcional
        except Exception as e:
            print(f"[ERRO] Falha ao ir para {edit_url}: {e}")
            continue

        try:
            product = collect_product_data(page, produto_id)
            products.append(product)
            storage.save(product)  # Ajuste para storage.save_product(product) se for o caso
        except Exception as e:
            print(f"[ERRO] Coleta falhou para {produto_id}: {e}")

    return products


# =========================
# 4) FUNÇÃO PRINCIPAL (CHAMADA NO MAIN)
# =========================
def collect_all_products(page: Page, storage) -> list[dict]:
    base_list_url = page.url

    # Passo 1: Coleta todos os edit_urls paginando
    edit_urls = collect_all_edit_urls(page, base_list_url)

    # Passo 2: Processa todos os produtos
    all_products = process_all_products(page, edit_urls, storage)

    print(f"[FIM] Total de produtos coletados: {len(all_products)}")

    return all_products