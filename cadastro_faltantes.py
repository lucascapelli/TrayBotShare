# arquivo: tray_sync_fixed.py
import os
import time
import shutil
from pathlib import Path
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

TMP_DIR = Path('./.tmp_tray_images')
TMP_DIR.mkdir(parents=True, exist_ok=True)


def get_env():
    return {
        'CDP_URL': os.getenv('CDP_URL', 'http://127.0.0.1:9222'),
        'SOURCE_PRODUCT_LIST_URL': os.getenv(
            'SOURCE_PRODUCT_LIST_URL',
            'https://www.grasiely.com.br/admin/products/list'
        ).strip(),
        'TARGET_PRODUCT_CREATE_URL': os.getenv(
            'TARGET_PRODUCT_CREATE_URL',
            'https://www.grasielyatacado.com.br/admin/products/create'
        ).strip(),
    }


PRODUCTS_TO_CREATE = [
    {
        'sku': '33.6an42',
        'name': 'Anel Masculino Chapa Personalizado Letra Inicial Banho de Ouro'
    },
]


# ---------- helpers ----------
def cleanup_tmp():
    try:
        if TMP_DIR.exists():
            shutil.rmtree(TMP_DIR)
    except Exception:
        pass


def download_images_to_files(img_urls, max_images=3):
    try:
        import requests
        from PIL import Image
        from io import BytesIO
    except Exception:
        return []

    local_files = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    for idx, src in enumerate(img_urls[:max_images]):
        try:
            if src.startswith('data:'):
                continue
            resp = requests.get(src, timeout=15, headers=headers)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content)).convert('RGB')
            fn = TMP_DIR / f"prod_img_{int(time.time())}_{idx}.png"
            img.save(fn, format='PNG')
            local_files.append(str(fn))
        except Exception as e:
            print(f"[WARN] Falha ao baixar imagem {src}: {e}")
            continue
    return local_files


def is_text_input(locator):
    try:
        attr = locator.get_attribute("type") or ""
        if attr.lower() in ("checkbox", "radio", "button", "submit", "image", "file"):
            return False
        return True
    except Exception:
        return True


def format_price_br(value):
    """Converte valor para formato brasileiro (ex: 1 -> 1,00)"""
    if not value:
        return ''
    try:
        num = float(value)
        return f"{num:.2f}".replace('.', ',')
    except:
        return str(value)


def fill_price_safe(page, value):
    if not value:
        return False
    candidates = [
        "input[placeholder='0,00']:not([type='checkbox']):not([type='radio'])",
        "input[inputmode='decimal']:not([type='checkbox']):not([type='radio'])",
        "input[type='text'][name*='price']:not([type='checkbox']):not([type='radio'])",
        "input[type='text'][id*='price']:not([type='checkbox']):not([type='radio'])",
        ".product-detail-price input:not([type='checkbox']):not([type='radio'])",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.count() and is_text_input(loc):
                try:
                    loc.click()
                except:
                    pass
                try:
                    loc.fill("")
                except:
                    pass
                try:
                    loc.fill(str(value))
                    print("[OK] Preço preenchido (selector):", sel)
                    return True
                except:
                    try:
                        page.evaluate("(el, v) => { el.value = v; el.dispatchEvent(new Event('input')); }", loc, str(value))
                        print("[OK] Preço preenchido via JS (selector):", sel)
                        return True
                    except:
                        continue
        except:
            continue

    # fallback genérico
    try:
        candidates2 = page.locator("input:not([type='checkbox']):not([type='radio'])")
        for i in range(candidates2.count()):
            el = candidates2.nth(i)
            try:
                p = el.get_attribute("placeholder") or ""
                name = el.get_attribute("name") or ""
                idv = el.get_attribute("id") or ""
                if '0,00' in p or 'price' in name.lower() or 'price' in idv.lower() or 'r$' in p.lower():
                    if is_text_input(el):
                        try:
                            el.fill(str(value))
                            print("[OK] Preço preenchido (fallback).")
                            return True
                        except:
                            continue
            except:
                continue
    except:
        pass

    print("[WARN] Campo de preço não encontrado/compatível.")
    return False


def fill_dimension(page, label, value):
    if not value:
        return
    try:
        fieldset = page.locator(f"fieldset:has(legend:has-text('{label}'))").first
        if fieldset.count():
            input_field = fieldset.locator("input.app-input-text__input").first
            if input_field.count():
                input_field.fill(str(value))
                print(f"[OK] {label} preenchido: {value}")
                return
        print(f"[WARN] Campo {label} não encontrado.")
    except Exception as e:
        print(f"[ERRO] {label}: {e}")


def select_vue_option_by_legend(page, legend_text, option_text):
    """Seleciona uma opção em um VueSelect localizado pelo legend."""
    if not option_text or option_text == "Selecionar":
        return
    try:
        fieldset = page.locator(f"fieldset:has(legend:has-text('{legend_text}'))").first
        if fieldset.count():
            # Abre o dropdown clicando no campo de busca ou na seta
            dropdown = fieldset.locator(".vs__dropdown-toggle, .vs__search").first
            if dropdown.count():
                dropdown.click()
                page.wait_for_timeout(500)
                # Tenta encontrar a opção
                option = page.locator(f"text={option_text}").first
                if option.count():
                    option.click()
                    print(f"[OK] {legend_text} selecionado: {option_text}")
                    return
        print(f"[WARN] Opção '{option_text}' para '{legend_text}' não encontrada.")
    except Exception as e:
        print(f"[ERRO] {legend_text}: {e}")


def select_category(page, category_text):
    """Seleciona categoria principal no destino via modal."""
    if not category_text:
        return
    try:
        # Localiza o botão "Adicionar categoria principal"
        btn = page.locator("button:has-text('Adicionar categoria principal')").first
        if not btn.count():
            btn = page.locator(".product-category button.btn-outline-primary").first
        if not btn.count():
            print("[WARN] Botão de adicionar categoria principal não encontrado.")
            return

        btn.click()
        print("[DEBUG] Botão 'Adicionar categoria principal' clicado.")
        
        # Aguarda o campo de busca aparecer (indicador de que o modal carregou)
        search_input = page.locator("input[placeholder*='Nome ou código']").first
        search_input.wait_for(state="visible", timeout=5000)
        page.wait_for_timeout(500)  # pequena estabilização

        # Tenta encontrar a categoria pelo texto exato e clicar no botão "Selecionar" associado
        xpath = f"//*[contains(text(), '{category_text}')]/ancestor::*[.//button[contains(text(), 'Selecionar')]]//button[contains(text(), 'Selecionar')]"
        select_btn = page.locator(f"xpath={xpath}").first
        if select_btn.count():
            select_btn.click()
            print(f"[OK] Categoria '{category_text}' selecionada via XPath.")
            page.wait_for_timeout(1000)
            return

        # Fallback: usa o campo de busca
        print(f"[DEBUG] Buscando por '{category_text}' no campo de busca...")
        search_input.fill(category_text)
        search_input.press('Enter')
        page.wait_for_timeout(1500)
        # Após buscar, clica no primeiro botão "Selecionar" que aparecer
        select_btn = page.locator("button:has-text('Selecionar')").first
        if select_btn.count():
            select_btn.click()
            print(f"[OK] Categoria '{category_text}' selecionada via busca.")
            page.wait_for_timeout(1000)
            return

        print(f"[WARN] Categoria '{category_text}' não encontrada no modal.")
    except Exception as e:
        print(f"[ERRO] Categoria: {e}")


# ---------- extração ----------
def extract_product_data(page, product):
    print("[INFO] Buscando produto...")
    data = {}

    # (código de busca e clique - igual ao anterior, mantido)
    try:
        page.wait_for_selector("table tbody tr", timeout=8000)
    except Exception:
        pass

    candidate_selectors = [
        'input[data-tray-tst="expanded-filter-product-input-product-name"]',
        "input[placeholder*='Nome']",
        "input[placeholder*='nome']",
        "input[type='search']",
        "input[placeholder*='Pesquisar']",
        "input[placeholder*='Buscar']",
    ]

    search_input = None
    for sel in candidate_selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                search_input = loc.first
                print(f"[DEBUG] Usando seletor de busca: {sel}")
                break
        except Exception:
            continue

    if search_input:
        try:
            search_input.fill(product['name'])
            search_input.press('Enter')
            page.wait_for_timeout(1200)
            try:
                page.wait_for_selector(f'text="{product["name"]}"', timeout=6000)
            except Exception:
                pass
        except Exception as e:
            print("[WARN] Falha ao usar campo de busca:", e)
    else:
        print("[WARN] Campo de busca não encontrado — procurando direto na tabela.")

    try:
        rows = page.locator('table tbody tr')
        total = rows.count()
        found = False
        for i in range(total):
            row = rows.nth(i)
            try:
                text = (row.inner_text() or "").lower()
            except Exception:
                text = ""
            if product['name'].lower() in text:
                print(f"[DEBUG] Produto encontrado na linha {i}. Tentando clicar editar...")
                edit_icon = row.locator('i.mdi-square-edit-outline, i[class*="mdi-square-edit-outline"]')
                if edit_icon.count() > 0:
                    try:
                        edit_icon.first.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        edit_icon.first.click(force=True)
                        found = True
                        break
                    except Exception as e:
                        print("[ERRO] Falha ao clicar ícone de editar:", e)
                else:
                    parent_clickable = row.locator('a:has(i.mdi-square-edit-outline), span:has(i.mdi-square-edit-outline), div:has(i.mdi-square-edit-outline)')
                    if parent_clickable.count() > 0:
                        try:
                            parent_clickable.first.click(force=True)
                            found = True
                            break
                        except Exception as e:
                            print("[ERRO] Falha ao clicar no elemento pai:", e)
        if not found:
            print(f"[ERRO] Produto '{product['name']}' não encontrado.")
            return {}
    except Exception as e:
        print("[ERRO] Falha ao iterar linhas:", e)
        return {}

    try:
        page.wait_for_url("**/admin/products/*/edit", timeout=7000)
    except Exception:
        try:
            page.wait_for_url("**/admin/products/*/update*", timeout=4000)
        except Exception:
            try:
                page.wait_for_selector(".product-info-detail, .product-images-video, .product-detail-price, h4:has-text('Descrição do produto')", timeout=10000)
            except Exception:
                page.wait_for_timeout(1500)

    # ========== EXTRAÇÃO DOS CAMPOS ==========

    # NOME
    try:
        name_candidates = [
            ".product-info-detail input",
            "fieldset.app-input-text input",
            "input[placeholder*='Camiseta']",
            "input[placeholder*='Nome']",
            "input[class*='app-input-text__input']",
            "input[name*='name']",
            "input[id*='name']",
        ]
        name_val = ''
        for sel in name_candidates:
            loc = page.locator(sel).first
            if loc.count():
                try:
                    name_val = loc.input_value()
                    if name_val:
                        break
                except Exception:
                    continue
        data['name'] = name_val or product['name']
    except Exception:
        data['name'] = product['name']

    # SKU
    try:
        sku_candidates = [
            "input[placeholder*='REF']",
            "input[placeholder*='Ex: REF-123']",
            "input[id*='reference']",
            "input[name*='reference']",
            "input[name*='sku']",
            "input[id*='sku']",
        ]
        sku_val = ''
        for sel in sku_candidates:
            loc = page.locator(sel).first
            if loc.count():
                try:
                    sku_val = loc.input_value()
                    if sku_val:
                        break
                except Exception:
                    continue
        data['sku'] = sku_val or product.get('sku', '')
    except Exception:
        data['sku'] = product.get('sku', '')

    # PREÇO (ajustado com seletor mais específico)
    price_val = ''
    # Tenta pelo legend "Preço de venda" e input com placeholder '0,00'
    try:
        # Localiza o fieldset que contém o legend "Preço de venda"
        price_fieldset = page.locator("fieldset:has(legend:has-text('Preço de venda'))").first
        if price_fieldset.count():
            # Dentro do fieldset, procura o input com placeholder '0,00'
            price_input = price_fieldset.locator("input[placeholder='0,00']").first
            if price_input.count():
                price_val = price_input.input_value()
                print(f"[DEBUG] Preço encontrado via legend 'Preço de venda': '{price_val}'")
    except Exception as e:
        print(f"[DEBUG] Erro ao buscar preço via legend: {e}")

    if not price_val:
        # Fallback: input com placeholder '0,00' em qualquer lugar (menos específico)
        try:
            price_input = page.locator("input[placeholder='0,00']").first
            if price_input.count():
                price_val = price_input.input_value()
                print(f"[DEBUG] Preço encontrado via placeholder '0,00': '{price_val}'")
        except:
            pass

    if not price_val:
        # Último fallback (evitar)
        candidates = [
            "input[name*='price']",
            "input[id*='price']",
            ".product-detail-price input"
        ]
        for sel in candidates:
            try:
                inp = page.locator(sel).first
                if inp.count():
                    price_val = inp.input_value()
                    if price_val:
                        print(f"[DEBUG] Preço encontrado via seletor '{sel}': '{price_val}'")
                        break
            except:
                continue
    data['price'] = price_val
    if not price_val:
        print("[WARN] Preço não encontrado na origem.")

    # ESTOQUE
    stock_val = ''
    try:
        estoque_fieldset = page.locator("fieldset:has(legend:has-text('Estoque'))").first
        if estoque_fieldset.count():
            estoque_input = estoque_fieldset.locator("input.app-input-text__input").first
            if estoque_input.count():
                stock_val = estoque_input.input_value()
                print(f"[DEBUG] Estoque encontrado via legend 'Estoque': '{stock_val}'")
    except Exception as e:
        print(f"[DEBUG] Erro ao buscar estoque: {e}")

    if not stock_val:
        candidates = [
            "input[placeholder='0']",
            "input[name*='stock']",
            "input[id*='stock']",
            ".product-stock input"
        ]
        for sel in candidates:
            try:
                inp = page.locator(sel).first
                if inp.count():
                    stock_val = inp.input_value()
                    if stock_val:
                        print(f"[DEBUG] Estoque encontrado via seletor '{sel}': '{stock_val}'")
                        break
            except:
                continue
    data['stock'] = stock_val
    if not stock_val:
        print("[WARN] Estoque não encontrado na origem.")

    # DIMENSÕES
    def get_dimension_by_legend(label):
        try:
            fieldset = page.locator(f"fieldset:has(legend:has-text('{label}'))").first
            if fieldset.count():
                input_field = fieldset.locator("input.app-input-text__input").first
                if input_field.count():
                    return input_field.input_value()
        except:
            pass
        return ''

    def get_dimension_by_suffix(suffix):
        try:
            elem = page.locator(f"div.input-group-text:has-text('{suffix}')").first
            if elem.count():
                fieldset = elem.locator("xpath=ancestor::fieldset")
                if fieldset.count():
                    input_field = fieldset.locator("input.app-input-text__input").first
                    if input_field.count():
                        return input_field.input_value()
        except:
            pass
        return ''

    data['weight'] = get_dimension_by_suffix("gr") or get_dimension_by_legend("Peso")
    print(f"[DEBUG] Peso extraído: '{data['weight']}'" if data['weight'] else "[DEBUG] Peso não encontrado")

    data['height'] = get_dimension_by_legend("Altura")
    print(f"[DEBUG] Altura extraída: '{data['height']}'" if data['height'] else "[DEBUG] Altura não encontrada")

    data['width'] = get_dimension_by_legend("Largura")
    print(f"[DEBUG] Largura extraída: '{data['width']}'" if data['width'] else "[DEBUG] Largura não encontrada")

    data['length'] = get_dimension_by_legend("Comprimento")
    print(f"[DEBUG] Comprimento extraído: '{data['length']}'" if data['length'] else "[DEBUG] Comprimento não encontrado")

    # TEMPO DE GARANTIA
    try:
        garantia_fieldset = page.locator("fieldset:has(legend:has-text('Tempo de garantia'))").first
        if garantia_fieldset.count():
            selected = garantia_fieldset.locator(".vs__selected").first
            if selected.count():
                data['warranty'] = selected.inner_text().strip()
                print(f"[DEBUG] Tempo de garantia: '{data['warranty']}'")
    except Exception as e:
        print(f"[DEBUG] Erro ao extrair garantia: {e}")

    # PRAZO DE DISPONIBILIDADE
    try:
        prazo_fieldset = page.locator("fieldset:has(legend:has-text('Prazo de disponibilidade'))").first
        if prazo_fieldset.count():
            selected = prazo_fieldset.locator(".vs__selected").first
            if selected.count():
                data['availability'] = selected.inner_text().strip()
                print(f"[DEBUG] Prazo de disponibilidade: '{data['availability']}'")
    except Exception as e:
        print(f"[DEBUG] Erro ao extrair prazo: {e}")

    # MENSAGEM ADICIONAL
    try:
        addm = page.locator("textarea[placeholder*='Desconto'], textarea[placeholder*='Desconto exclusivo']").first
        data['additional_message'] = addm.input_value() if addm.count() else ''
        print(f"[DEBUG] Mensagem adicional: '{data['additional_message']}'")
    except Exception:
        data['additional_message'] = ''

    # ITENS INCLUSOS
    try:
        inc = page.locator("textarea[placeholder*='Ex: 1 adesivo']").first
        data['included_items'] = inc.input_value() if inc.count() else ''
        print(f"[DEBUG] Itens inclusos: '{data['included_items']}'")
    except Exception:
        data['included_items'] = ''

    # DESCRIÇÃO (CKEditor)
    try:
        if page.query_selector("iframe.cke_wysiwyg_frame, iframe[title*='Editor']"):
            frame = page.frame_locator("iframe.cke_wysiwyg_frame, iframe[title*='Editor']")
            body = frame.locator("body")
            data['description'] = body.inner_html() if body.count() else ''
        else:
            desc_container = page.locator("h4:has-text('Descrição do produto')").locator("xpath=ancestor::fieldset").locator("div")
            data['description'] = desc_container.inner_text() if desc_container.count() else ''
    except Exception:
        data['description'] = ''

    # IMAGENS
    try:
        imgs = page.locator("img.preview-image, .input-file-placeholder img, .preview-item img")
        img_srcs = []
        for i in range(imgs.count()):
            try:
                src = imgs.nth(i).get_attribute("src")
            except Exception:
                src = None
            if src and not src.startswith("data:"):
                img_srcs.append(src)
        data['images'] = img_srcs
    except Exception:
        data['images'] = []

    # CATEGORIA
    try:
        cat_val = ''
        # Tenta pegar o texto da categoria selecionada (pode estar em .product-category button.btn-outline-primary)
        cat_btn = page.locator(".product-category button.btn-outline-primary").first
        if cat_btn.count():
            cat_val = cat_btn.inner_text().strip()
        if not cat_val:
            cat_elem = page.locator(".product-category_text, .vs__selected").first
            if cat_elem.count():
                cat_val = cat_elem.inner_text().strip()
        data['category'] = cat_val
    except Exception:
        data['category'] = ''

    data['min_stock'] = ''

    print("[INFO] Extração concluída:")
    for k, v in data.items():
        if k == 'description':
            print(f"  {k}: {v[:100]}..." if v else f"  {k}: (vazio)")
        else:
            print(f"  {k}: {v}")
    return data


# ---------- criação ----------
def create_product(page, create_url, product_data):
    print("[INFO] Indo para cadastro...", create_url)
    try:
        page.goto(create_url, wait_until="domcontentloaded")
    except Exception as e:
        print("[ERRO] Falha ao navegar para criação:", e)
        return

    page.wait_for_timeout(900)

    # Nome
    try:
        name_locator = page.locator(
            ".product-info-detail input, "
            "fieldset.app-input-text input, "
            "input[placeholder*='Camiseta'], "
            "input[name*='name']"
        ).first
        if name_locator.count():
            name_locator.fill(product_data.get('name', ''))
            name_locator.press('Tab')
            print("[OK] Nome preenchido.")
        else:
            print("[WARN] Campo nome não encontrado.")
    except Exception as e:
        print("[ERRO] Nome:", e)

    # Imagens
    try:
        file_input = page.locator(
            "input[type='file'][accept^='image'], input[type='file']"
        ).first

        if file_input.count() and product_data.get('images'):
            local_files = download_images_to_files(
                product_data.get('images', []),
                max_images=3
            )
            if local_files:
                try:
                    file_input.set_input_files(local_files)
                    page.wait_for_timeout(700)
                    print(f"[OK] {len(local_files)} imagens enviadas.")
                except Exception as e:
                    print("[ERRO] Falha ao enviar imagens:", e)
            else:
                print("[WARN] Não há imagens baixadas para upload.")
        else:
            print("[INFO] Campo input file não encontrado ou sem imagens.")
    except Exception as e:
        print("[ERRO] Upload imagens:", e)

    # Categoria
    select_category(page, product_data.get('category', ''))

    # Descrição
    try:
        if page.query_selector("iframe.cke_wysiwyg_frame, iframe[title*='Editor']"):
            html = product_data.get('description', '')
            page.eval_on_selector(
                "iframe.cke_wysiwyg_frame, iframe[title*='Editor']",
                "(el, html) => { el.contentDocument.body.innerHTML = html; }",
                html
            )
            page.wait_for_timeout(300)
            print("[OK] Descrição escrita no editor (iframe).")
        else:
            t = page.locator(
                "textarea[name*='description'], textarea[id*='description']"
            ).first
            if t.count():
                t.fill(product_data.get('description', ''))
                print("[OK] Descrição escrita em textarea.")
    except Exception as e:
        print("[ERRO] Descrição:", e)

    # SKU
    try:
        sku_sel = page.locator(
            "input[placeholder*='REF'], "
            "input[name*='reference'], "
            "input[name*='sku']"
        ).first

        if sku_sel.count():
            sku_sel.fill(product_data.get('sku', ''))
            print("[OK] Referência preenchida.")
        else:
            print("[WARN] Campo referência não encontrado.")
    except Exception as e:
        print("[ERRO] Referência:", e)

    # Preço (formatado)
    price_value = format_price_br(product_data.get('price', ''))
    try:
        ok = fill_price_safe(page, price_value)
        if not ok:
            print("[WARN] Preço não preenchido.")
    except Exception as e:
        print("[ERRO] Preço:", e)

    # Estoque
    try:
        estoque_fieldset = page.locator("fieldset:has(legend:has-text('Estoque'))").first
        if estoque_fieldset.count():
            estoque_input = estoque_fieldset.locator("input.app-input-text__input").first
            if estoque_input.count():
                estoque_input.fill(product_data.get('stock', ''))
                print("[OK] Estoque preenchido.")
        else:
            print("[WARN] Campo estoque não encontrado.")
    except Exception as e:
        print("[ERRO] Estoque:", e)

    # Peso e dimensões
    fill_dimension(page, "Peso", product_data.get('weight', ''))
    fill_dimension(page, "Altura", product_data.get('height', ''))
    fill_dimension(page, "Largura", product_data.get('width', ''))
    fill_dimension(page, "Comprimento", product_data.get('length', ''))

    # Tempo de garantia
    select_vue_option_by_legend(page, "Tempo de garantia", product_data.get('warranty', ''))

    # Prazo de disponibilidade
    select_vue_option_by_legend(page, "Prazo de disponibilidade", product_data.get('availability', ''))

    # Itens inclusos
    try:
        itens_input = page.locator("textarea[placeholder*='Ex: 1 adesivo']").first
        if itens_input.count() and product_data.get('included_items'):
            itens_input.fill(product_data['included_items'])
            print("[OK] Itens inclusos preenchidos.")
    except Exception as e:
        print("[ERRO] Itens inclusos:", e)

    # Mensagem adicional
    try:
        msg_textarea = page.locator("textarea[placeholder*='Desconto exclusivo']").first
        if msg_textarea.count() and product_data.get('additional_message'):
            msg_textarea.fill(product_data['additional_message'])
            print("[OK] Mensagem adicional preenchida.")
    except Exception as e:
        print("[ERRO] Mensagem adicional:", e)

    # Salvar com tratamento do modal
    try:
        save_selector = (
            "button[data-tray-tst='page-product-detail-save'], "
            "button:has-text('Salvar')"
        )

        save_btn = page.locator(save_selector).first

        if not save_btn.count():
            print("[WARN] Botão salvar não encontrado.")
            return

        # remove disabled se houver
        try:
            page.eval_on_selector(
                save_selector,
                "el => el.removeAttribute('disabled')"
            )
        except Exception:
            pass

        # primeiro clique
        save_btn.click(force=True)
        page.wait_for_timeout(1200)

        # verifica modal
        modal = page.locator("#product-detail-modal")
        if modal.count() and modal.is_visible():
            print("[INFO] Modal detectado. Fechando...")

            close_btn = page.locator(
                "#product-detail-modal button.close"
            ).first

            if close_btn.count():
                close_btn.click(force=True)
            else:
                page.evaluate("""
                    const m = document.querySelector('#product-detail-modal');
                    if (m) m.style.display = 'none';
                """)

            try:
                modal.wait_for(state="hidden", timeout=5000)
            except Exception:
                page.wait_for_timeout(800)

            # segundo clique
            save_btn.click(force=True)
            print("[OK] Segundo clique em salvar executado.")

        # confirmação
        try:
            page.wait_for_selector(
                ".alert-success, .toast-success",
                timeout=6000
            )
            print("[SUCESSO] Produto salvo com sucesso.")
        except Exception:
            print("[INFO] Clique executado; confirme manualmente.")

    except Exception as e:
        print("[ERRO] Processo de salvar:", e)

    print("[INFO] Fluxo de criação finalizado.")


# ---------- main ----------
def main():
    env = get_env()
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(env['CDP_URL'])
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        pages = context.pages

        print("[DEBUG] Abas abertas:")
        for idx, pg in enumerate(pages):
            print(f"  Aba {idx}: {pg.url}")

        if len(pages) == 0:
            print("[ERRO] Nenhuma aba encontrada no contexto CDP.")
            return

        # localizar aba com a listagem de origem
        page_origem = None
        for pg in pages:
            try:
                if env['SOURCE_PRODUCT_LIST_URL'] in (pg.url or ''):
                    page_origem = pg
                    break
            except Exception:
                continue

        if not page_origem:
            page_origem = pages[0]
            try:
                print("[DEBUG] Abrindo SOURCE_PRODUCT_LIST_URL:", env['SOURCE_PRODUCT_LIST_URL'])
                page_origem.goto(env['SOURCE_PRODUCT_LIST_URL'], wait_until="domcontentloaded")
                page_origem.wait_for_selector("table tbody tr", timeout=10000)
            except Exception as e:
                print("[WARN] Ao navegar para SOURCE_PRODUCT_LIST_URL:", e)

        admin_create_url = env.get('TARGET_PRODUCT_CREATE_URL')

        # localizar/abrir aba destino (create)
        page_destino = None
        for pg in pages:
            try:
                if admin_create_url in (pg.url or ''):
                    page_destino = pg
                    break
            except Exception:
                continue

        if not page_destino:
            page_destino = context.new_page()
            try:
                page_destino.goto(admin_create_url, wait_until="domcontentloaded")
            except Exception as e:
                print("[WARN] Não foi possível abrir a URL de criação diretamente:", e)

        print("[DEBUG] Usando origem:", page_origem.url)
        print("[DEBUG] Usando destino:", page_destino.url if page_destino else admin_create_url)

        for prod in PRODUCTS_TO_CREATE:
            print("[INFO] Processando:", prod['name'])
            product_data = extract_product_data(page_origem, prod)
            if product_data:
                create_product(page_destino, admin_create_url, product_data)
            else:
                print("[WARN] Dados do produto vazios, pulando cadastro.")

    cleanup_tmp()


if __name__ == "__main__":
    main()