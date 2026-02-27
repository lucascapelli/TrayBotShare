import re
import json
import time
from urllib.parse import urljoin
from patchright.sync_api import Page
from typing import List, Tuple

# =========================
# 1) COLETA DO JSON DA EDIÇÃO - COM espera explícita e retry
# =========================
def collect_product_data(page: Page, produto_id: str, timeout: int = 15000) -> dict:
    """
    Captura o JSON de detalhe do produto usando um response listener (modo antigo, mais robusto).
    timeout em ms.
    """
    product = {"produto_id": produto_id}
    detail_json = None

    def handle_response(response):
        nonlocal detail_json
        # se já capturamos, ignora
        if detail_json:
            return
        try:
            ct = response.headers.get("content-type", "")
            if "application/json" not in ct:
                return
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                rid = data["data"].get("id")
                if rid is None:
                    return
                if str(rid) == str(produto_id):
                    detail_json = data["data"]
                    print(f"✅ JSON DETALHE CAPTURADO para {produto_id}")
        except Exception:
            # silencioso — não queremos quebrar por causa de um response malformado
            return

    page.on("response", handle_response)

    try:
        # navega (networkidle costuma garantir que as chamadas XHR acabem, mas mantemos o listener)
        page.goto(f"https://www.grasiely.com.br/admin/products/{produto_id}/edit",
                  wait_until="networkidle", timeout=timeout)
    except Exception:
        # podemos ignorar timeout aqui porque o listener ainda pode capturar a resposta
        pass

    # polling curto até timeout
    waited = 0
    interval = 250  # ms
    while detail_json is None and waited < timeout:
        page.wait_for_timeout(interval)
        waited += interval

    page.remove_listener("response", handle_response)

    if not detail_json:
        print(f"⚠️ Timeout/erro no produto {produto_id}: não capturou JSON em {timeout}ms")
        return product

    d = detail_json
    product.update({
        "nome": d.get("name"),
        "preco": float(d.get("price", "0").replace(",", ".")) if d.get("price") else None,
        "descricao": d.get("description"),
        "estoque": d.get("stock"),
        "estoque_minimo": d.get("minimum_stock"),
        "categoria": d.get("category_name"),
        "referencia": d.get("reference"),
        "peso": d.get("weight"),
        "altura": d.get("height"),
        "largura": d.get("width"),
        "comprimento": d.get("length"),
        "imagem_url": (d.get("ProductImage") or [{}])[0].get("https") if d.get("ProductImage") else None,
        "notificacao_estoque_baixo": d.get("minimum_stock_alert") == "1",
        "itens_inclusos": d.get("included_items"),
        "mensagem_adicional": d.get("additional_message"),
        "tempo_garantia": d.get("warranty"),
        "seo_preview": {
            "link": (d.get("url") or {}).get("https"),
            "title": next((m.get("content") for m in d.get("metatag", []) if m.get("type") == "title"), None),
            "description": next((m.get("content") for m in d.get("metatag", []) if m.get("type") == "description"), None)
        }
    })
    return product

# =========================
# 2) CAPTURA IDS - PAGINAÇÃO ROBUSTA (com limite de tentativas de scroll)
# =========================
def collect_all_edit_urls(page: Page, base_list_url: str) -> List[Tuple[str, str]]:
    """
    Tenta interceptar respostas de listagem paginada. Usa:
     • expect_response para a primeira página
     • tentativa de clicar botão 'next' via vários seletores
     • fallback para scroll infinito/dom extraction se necessário
    Retorna lista de (produto_id, url_edit).
    """
    all_ids = set()
    captured_responses = []

    def is_list_response(response):
        try:
            return ("products-search" in response.url or "/api/products" in response.url) \
                   and response.status == 200 \
                   and "application/json" in response.headers.get("content-type", "")
        except:
            return False

    print("Iniciando coleta via interceptação com paginação...")
    # captura primeira página
    try:
        with page.expect_response(is_list_response, timeout=10000) as r:
            page.goto(base_list_url, wait_until="networkidle", timeout=30000)
        response = r.value
        data = response.json()
        if data.get("data"):
            captured_responses.append(data)
            print(f"Página 1: {len(data['data'])} produtos")
    except Exception as e:
        print(f"⚠️ Não capturou resposta da primeira página: {e}")
        # continua para tentar extrair via DOM

    max_pages = 300
    current_page = 1

    # novo: contador de tentativas de fallback (scroll/extraction)
    scroll_attempts = 0
    max_scroll_attempts = 15  # <-- máximo de vezes que imprimirá o fallback antes de desistir

    while current_page < max_pages:
        # tentativa de localizar botão next por várias estratégias
        clicked = False
        try:
            # 1) tentativa por aria/role
            try:
                next_btn = page.get_by_role("button", name=re.compile(r"next|próxima|>',',',", re.I))
            except:
                next_btn = None
            # 2) seletor comum
            selectors = [
                "a.next:not(.disabled)",
                "button.next:not([disabled])",
                ".pagination a[rel='next']:not(.disabled)",
                "a[aria-label*='next']:not([aria-disabled='true'])",
                "button[aria-label*='next']:not([aria-disabled='true'])",
                "a:has-text('Próxima'):not(.disabled)",
                "a:has-text('Next'):not(.disabled)",
            ]
            # try role-based click first if exists and visible
            if next_btn and getattr(next_btn, "count", lambda: 1)() > 0:
                try:
                    if next_btn.is_visible():
                        current_page += 1
                        print(f"Ir para página {current_page} (role click)...")
                        with page.expect_response(is_list_response, timeout=15000) as r:
                            next_btn.click()
                        response = r.value
                        data = response.json()
                        if data.get("data"):
                            captured_responses.append(data)
                            print(f"Página {current_page}: {len(data['data'])} produtos")
                            clicked = True
                            scroll_attempts = 0
                            continue
                except Exception:
                    pass

            # try selectors
            for sel in selectors:
                try:
                    locator = page.locator(sel)
                    if locator.count() > 0:
                        loc = locator.first
                        if loc.is_visible():
                            current_page += 1
                            print(f"Ir para página {current_page} (selector '{sel}')...")
                            with page.expect_response(is_list_response, timeout=15000) as r:
                                loc.click()
                            response = r.value
                            data = response.json()
                            if data.get("data"):
                                captured_responses.append(data)
                                print(f"Página {current_page}: {len(data['data'])} produtos")
                            clicked = True
                            scroll_attempts = 0
                            break
                except Exception:
                    continue
        except Exception as e:
            print(f"[WARN] Erro ao tentar localizar/clicar next: {e}")

        if clicked:
            # continue paginando
            continue

        # fallback: scroll infinito / extrair mais do DOM
        scroll_attempts += 1
        if scroll_attempts > max_scroll_attempts:
            print(f"[INFO] Alcançado máximo de {max_scroll_attempts} tentativas de scroll/extracão - finalizando paginação")
            break

        print(f"[INFO] ({scroll_attempts}/{max_scroll_attempts}) Sem botão next ou clique falhou. Tentando scroll / extração DOM...")
        previous_count = len(captured_responses)
        # força scroll para tentar acionar APIs
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)
        except Exception:
            pass

        # tenta extrair via JS os ids que já estão no DOM
        try:
            ids_on_page = page.evaluate("""
                () => {
                    const ids = [];
                    document.querySelectorAll('a[href*="/products/"][href*="/edit"]').forEach(link => {
                        const m = link.href.match(/\\/products\\/(\\d+)\\/edit/);
                        if(m) ids.push(m[1]);
                    });
                    document.querySelectorAll('[data-id]').forEach(el => {
                        const id = el.getAttribute('data-id');
                        if(id && /^\\d+$/.test(id)) ids.push(id);
                    });
                    document.querySelectorAll('[data-product-id]').forEach(el => {
                        const id = el.getAttribute('data-product-id');
                        if(id && /^\\d+$/.test(id)) ids.push(id);
                    });
                    return [...new Set(ids)];
                }
            """)
            found_new_ids = False
            for pid in ids_on_page:
                if str(pid) not in all_ids:
                    found_new_ids = True
                all_ids.add(str(pid))

            # se achou novos ids no DOM, resetar contador de attempts
            if found_new_ids:
                scroll_attempts = 0

            if len(captured_responses) == previous_count and not ids_on_page:
                print("[INFO] Sem novos dados após scroll/extracão DOM - finalizando paginação")
                break
            # se conseguirmos ids direto, continuar tentando scroll até estabilizar
            # limite de segurança
            if len(all_ids) > 0 and len(captured_responses) == 0:
                # se nunca capturou via API mas tem ids DOM, adiciona um pseudo-captured set
                captured_responses.append({"data": [{"id": pid} for pid in list(all_ids)]})
        except Exception as e:
            print(f"[WARN] extração DOM falhou: {e}")
            break

    # processa respostas api capturadas
    for data in captured_responses:
        for item in data.get("data", []):
            pid = str(item.get("id"))
            if pid:
                all_ids.add(pid)

    all_ids_list = sorted(list(all_ids))
    print(f"Total de {len(all_ids_list)} produtos únicos capturados")
    return [(pid, f"https://www.grasiely.com.br/admin/products/{pid}/edit") for pid in all_ids_list]


# =========================
# 3) PROCESSA - agora com logs e tentativa de salvar via storage
# =========================
def process_all_products(page: Page, edit_urls: list, storage, batch_size: int = 20) -> list:
    products = []
    total = len(edit_urls)
    errors = 0
    buffer = []

    print(f"Iniciando processamento de {total} produtos...")
    for idx, (pid, url) in enumerate(edit_urls, 1):
        if idx % 50 == 0 or idx == 1:
            print(f"Progresso: {idx}/{total} ({(idx/total)*100:.1f}%) | Erros: {errors}")

        try:
            product = collect_product_data(page, pid)
            if product.get("nome"):
                products.append(product)
                buffer.append(product)
                if len(buffer) >= batch_size:
                    try:
                        storage.save_many(buffer)
                    except AttributeError:
                        for p in buffer:
                            try:
                                storage.save(p)
                            except Exception:
                                pass
                    buffer = []
                if idx % 10 == 0:
                    print(f"✓ {idx}: {pid} - {product.get('nome', 'N/A')[:40]}")
            else:
                errors += 1
                print(f"⚠️ {idx}: {pid} - SEM DADOS")
        except Exception as e:
            errors += 1
            print(f"❌ {idx}: {pid} - ERRO: {str(e)[:80]}")

    # salva o buffer restante
    if buffer:
        try:
            storage.save_many(buffer)
        except AttributeError:
            for p in buffer:
                try:
                    storage.save(p)
                except Exception:
                    pass

    print(f"Processamento concluído: {len(products)} produtos salvos | {errors} erros")
    return products


# =========================
# 4) PRINCIPAL (exposição pública)
# =========================
def collect_all_products(page: Page, storage) -> list:
    base_list_url = "https://www.grasiely.com.br/admin/products/list?sort=name&page[size]=25&page[number]=1"

    print("\nETAPA 1: COLETANDO IDS DOS PRODUTOS")
    edit_urls = collect_all_edit_urls(page, base_list_url)

    # Limite para teste rápido: só 5 produtos
    edit_urls = edit_urls[:5]  # <-- comentário: limitar coleta para teste rápido

    if not edit_urls:
        print("⚠️ Nenhum produto foi capturado!")
        return []

    print("\nETAPA 2: COLETANDO DADOS DETALHADOS")
    all_products = process_all_products(page, edit_urls, storage,batch_size=1)

    print("\nCONCLUÍDO")
    print(f"Total encontrado: {len(edit_urls)} produtos")
    print(f"Total coletado: {len(all_products)} produtos")
    print(f"Falhas: {len(edit_urls) - len(all_products)}")
    return all_products