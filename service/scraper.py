# service/scraper.py

import re
from playwright.sync_api import Page


def collect_product_data(page: Page) -> dict:
    product = {}

    def log(field, value):
        print(f"[COLETA] {field}: {value}")

    # ========================
    # 1) CLICA NA REFERÊNCIA
    # ========================
    ref_locator = page.locator("text=/Ref\\.\\s*(.+)/").first

    if ref_locator.count() == 0:
        raise Exception("Nenhuma referência encontrada na listagem")

    ref_text = ref_locator.inner_text().strip()
    product["referencia"] = ref_text.replace("Ref.", "").strip()
    log("referencia", product["referencia"])

    ref_locator.click()

    # ========================
    # 2) CLICA NO LINK DE EDIÇÃO
    # ========================
    edit_link = page.get_by_role("link").filter(
        has_text=re.compile(r"^$")
    ).nth(1)

    edit_link.click()

    # ESPERA ELEMENTO REAL DA TELA (NÃO networkidle)
    name_input = page.get_by_role(
        "textbox",
        name="Camiseta de manga longa"
    )
    name_input.wait_for(timeout=20000)

    # ========================
    # 3) TELA DE EDIÇÃO
    # ========================

    # NOME
    product["nome"] = name_input.input_value()
    log("nome", product["nome"])

    # IMAGEM
    img = page.get_by_role("img").first
    product["imagem_url"] = img.get_attribute("src")
    log("imagem_url", product["imagem_url"])

    # CATEGORIA
    categoria_btn = page.get_by_role(
        "button",
        name="PINGENTE"
    )
    product["categoria"] = categoria_btn.inner_text().strip()
    log("categoria", product["categoria"])

    # DESCRIÇÃO (iframe editor1)
    iframe = page.frame_locator(
        "iframe[title='Editor de Rich Text, editor1']"
    )

    try:
        product["descricao"] = iframe.locator("body").inner_text()
    except:
        product["descricao"] = None

    log("descricao", product["descricao"])

    # SEO
    seo = page.locator(".product-seo > .product-seo__preview")
    product["seo_preview"] = seo.inner_text() if seo.count() > 0 else None
    log("seo_preview", product["seo_preview"])

    # PREÇO
    product["preco"] = (
        page.get_by_role("group", name="Preço de venda")
        .get_by_placeholder("0,00")
        .input_value()
    )
    log("preco", product["preco"])

    # ESTOQUE
    product["estoque"] = (
        page.get_by_role("group", name="Estoque", exact=True)
        .get_by_placeholder("0")
        .input_value()
    )
    log("estoque", product["estoque"])

    product["estoque_minimo"] = (
        page.get_by_role("group", name="Estoque mínimo")
        .get_by_placeholder("0")
        .input_value()
    )
    log("estoque_minimo", product["estoque_minimo"])

    # CHECKBOX NOTIFICAÇÃO ESTOQUE BAIXO
    checkbox_label = page.get_by_text("Notificação de estoque baixo")
    checkbox_input = checkbox_label.locator(
        "xpath=ancestor::label//input"
    )

    if checkbox_input.count() > 0:
        product["notificacao_estoque_baixo"] = checkbox_input.is_checked()
    else:
        product["notificacao_estoque_baixo"] = False

    log("notificacao_estoque_baixo",
        product["notificacao_estoque_baixo"])

    # REFERÊNCIA PERSONALIZADA
    product["referencia_personalizada"] = (
        page.get_by_role("textbox", name="Ex: REF-")
        .input_value()
    )
    log("referencia_personalizada",
        product["referencia_personalizada"])

    # DIMENSÕES
    product["peso"] = (
        page.get_by_role("group", name="Peso")
        .get_by_placeholder("0")
        .input_value()
    )
    log("peso", product["peso"])

    product["altura"] = (
        page.get_by_role("group", name="Altura")
        .get_by_placeholder("0")
        .input_value()
    )
    log("altura", product["altura"])

    product["largura"] = (
        page.get_by_role("group", name="Largura")
        .get_by_placeholder("0", exact=True)
        .input_value()
    )
    log("largura", product["largura"])

    product["comprimento"] = (
        page.get_by_role("group", name="Comprimento")
        .get_by_placeholder("0", exact=True)
        .input_value()
    )
    log("comprimento", product["comprimento"])

    # TEMPO DE GARANTIA
    garantia_group = page.get_by_role(
        "group",
        name="Tempo de garantia"
    )
    product["tempo_garantia"] = garantia_group.inner_text()
    log("tempo_garantia", product["tempo_garantia"])

    product["tempo_garantia_personalizado"] = (
        page.get_by_role(
            "textbox",
            name="Descreva o tempo de garantia"
        ).input_value()
    )
    log("tempo_garantia_personalizado",
        product["tempo_garantia_personalizado"])

    # ITENS INCLUSOS
    itens_boxes = page.get_by_role(
        "textbox",
        name="Ex: 1 adesivo"
    )

    product["itens_inclusos"] = itens_boxes.nth(0).input_value()
    log("itens_inclusos", product["itens_inclusos"])

    product["mensagem_adicional"] = itens_boxes.nth(1).input_value()
    log("mensagem_adicional",
        product["mensagem_adicional"])

    return product