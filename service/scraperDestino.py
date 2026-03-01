# scraperDestino.py
import os
import re
import json
import time
from urllib.parse import urljoin
from patchright.sync_api import Page
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime

# =========================
# CONFIGURAÇÕES OTIMIZADAS
# =========================
@dataclass
class ScraperConfig:
    timeout_per_product: int = 12000  # 12s por produto (OTIMIZADO: era 20s)
    max_retries: int = 2  # 2 tentativas (OTIMIZADO: era 3)
    retry_delay: int = 1500  # 1.5s entre tentativas (OTIMIZADO: era 2s)
    batch_size: int = 50  # Salvar a cada 50 produtos (OTIMIZADO: era 20)
    max_pages: int = 300
    max_scroll_attempts: int = 15
    page_size: int = 25
    test_mode: bool = True  # ✅ MODO TESTE: True = apenas produtos da ORIGEM | False = todos
    test_limit: int = 5  # ✅ Quantos produtos no modo teste (só usado se não conseguir carregar origem)

CONFIG = ScraperConfig()

# =========================
# UTILIDADES
# =========================
class ProgressTracker:
    def __init__(self, total: int):
        self.total = total
        self.success = 0
        self.failed = 0
        self.retries = 0
        self.start_time = time.time()
        self.failed_ids = []
    
    def log_success(self, pid: str, name: str):
        self.success += 1
        if self.success % 10 == 0 or self.success == 1:
            self._print_progress(f"✓ {pid} - {name[:40]}")
    
    def log_failure(self, pid: str, reason: str):
        self.failed += 1
        self.failed_ids.append(pid)
        if len(self.failed_ids) <= 20:
            print(f"❌ [{self.current}/{self.total}] {pid} - {reason[:60]}")
    
    def log_retry(self, pid: str, attempt: int):
        self.retries += 1
        if self.retries % 10 == 0:
            print(f"🔄 {self.retries} retries até agora...")
    
    @property
    def current(self):
        return self.success + self.failed
    
    def _print_progress(self, detail: str = ""):
        elapsed = time.time() - self.start_time
        rate = self.success / elapsed if elapsed > 0 else 0
        eta = (self.total - self.current) / rate if rate > 0 else 0
        
        progress = (self.current / self.total * 100) if self.total > 0 else 0
        print(f"[{self.current}/{self.total}] {progress:.1f}% | "
              f"✓{self.success} ❌{self.failed} 🔄{self.retries} | "
              f"⏱️{elapsed/60:.1f}min | ETA: {eta/60:.1f}min ({rate*60:.1f} prod/min)")
        if detail:
            print(f"  {detail}")
    
    def print_summary(self):
        elapsed = time.time() - self.start_time
        print("\n" + "="*60)
        print("RESUMO DA COLETA")
        print("="*60)
        print(f"Total produtos: {self.total}")
        print(f"Sucessos: {self.success} ({self.success/self.total*100:.1f}%)")
        print(f"Falhas: {self.failed} ({self.failed/self.total*100:.1f}%)")
        print(f"Retries: {self.retries}")
        print(f"Tempo total: {elapsed/60:.1f} minutos")
        print(f"Taxa: {self.success/elapsed*60:.1f} produtos/min")
        
        if self.failed_ids:
            print(f"\nProdutos que falharam ({len(self.failed_ids)}):")
            print(", ".join(self.failed_ids[:20]))
            if len(self.failed_ids) > 20:
                print(f"... e mais {len(self.failed_ids) - 20}")
        print("="*60)

def clean_html(html_text: str) -> str:
    if not html_text:
        return ""
    return BeautifulSoup(html_text, "html.parser").get_text(separator="\n").strip()

def safe_float(value, default=None) -> Optional[float]:
    if not value:
        return default
    try:
        return float(str(value).replace(",", "."))
    except (ValueError, AttributeError):
        return default

# =========================
# ✅ FUNÇÃO NOVA: CARREGA NOMES DA ORIGEM
# =========================
def load_origem_product_names() -> List[str]:
    try:
        origem_path = "produtos/ProdutosOrigem.json"
        if not os.path.exists(origem_path):
            print(f"⚠️ Arquivo {origem_path} não encontrado. Coletando produtos normalmente.")
            return []
        
        with open(origem_path, 'r', encoding='utf-8') as f:
            produtos_origem = json.load(f)
        
        nomes = [p.get("nome", "").strip() for p in produtos_origem if p.get("nome")]
        print(f"✅ {len(nomes)} nomes carregados da ORIGEM para filtrar")
        return nomes
    except Exception as e:
        print(f"❌ Erro ao carregar nomes da ORIGEM: {str(e)}")
        return []

# =========================
# 🔥 NOVA FUNÇÃO: BUSCA DIRETA POR NOME (MODO TESTE - RECOMENDADA!)
# =========================
def collect_matched_ids_via_search(page: Page, origem_names: List[str]) -> List[str]:
    """
    Busca cada nome da ORIGEM diretamente na barra de pesquisa do DESTINO.
    MUITO mais rápido e confiável que paginar + abrir 200+ páginas de edição.
    """
    if not origem_names:
        return []
    
    print(f"\n🔍 BUSCA RÁPIDA NA BARRA DE PESQUISA: Procurando {len(origem_names)} nomes da ORIGEM...")
    
    matched_ids = []
    base_list_url = "https://www.grasielyatacado.com.br/admin/products/list?sort=name&page[size]=25&page[number]=1"
    
    # Garante que estamos na listagem
    page.goto(base_list_url, wait_until="networkidle", timeout=15000)
    
    # Localizador da barra de busca (exatamente como você indicou)
    search_locator = page.get_by_role("textbox", name="Buscar por nome, código,")
    
    for idx, nome_origem in enumerate(origem_names, 1):
        try:
            print(f"   [{idx}/{len(origem_names)}] Buscando: {nome_origem[:70]}...")
            
            # Limpa e preenche a busca
            search_locator.clear()
            search_locator.fill(nome_origem.strip())
            
            # Intercepta a resposta da API de busca
            with page.expect_response(
                lambda response: (
                    response.status == 200 and
                    "application/json" in (response.headers.get("content-type", "") or "") and
                    ("/api/products" in response.url or "products-search" in response.url or "/products/search" in response.url)
                ),
                timeout=10000
            ) as response_info:
                page.keyboard.press("Enter")
            
            response = response_info.value
            data = response.json()
            
            if data.get("data"):
                for item in data["data"]:
                    pid = str(item.get("id"))
                    item_name = (item.get("name") or "").strip()
                    
                    # Match flexível (ignora maiúsculas, acentos leves e ordem)
                    if pid and (
                        nome_origem.lower() in item_name.lower() or 
                        item_name.lower() in nome_origem.lower()
                    ):
                        if pid not in matched_ids:
                            matched_ids.append(pid)
                            print(f"      ✅ ENCONTRADO: {pid} → {item_name[:80]}")
                            break  # um match por nome é suficiente
        
        except Exception as e:
            print(f"      ⚠️ Erro na busca '{nome_origem[:50]}': {str(e)[:80]}")
            continue  # continua para o próximo nome
    
    # Remove duplicatas e ordena numericamente
    matched_ids = sorted(list(set(matched_ids)), key=lambda x: int(x) if x.isdigit() else 0)
    
    print(f"\n🎉 {len(matched_ids)} produtos encontrados via busca direta!")
    if len(matched_ids) == 0:
        print("   ⚠️ Nenhum match encontrado. Verifique se os nomes da ORIGEM estão escritos igual no DESTINO.")
    
    return matched_ids

# =========================
# ✅ FUNÇÃO ANTIGA (mantida para possível uso futuro)
# =========================
def filter_ids_by_origin_names(page: Page, all_product_ids: List[str], origem_names: List[str]) -> List[str]:
    """Versão antiga (lenta) - mantida apenas como fallback"""
    if not origem_names:
        return all_product_ids
    print(f"\n🔍 [VERSÃO ANTIGA] Filtrando {len(all_product_ids)} IDs...")
    # ... (código antigo mantido, mas NÃO será usado no test_mode)
    matched_ids = []
    # ... (todo o código antigo aqui - omitido por brevidade)
    return matched_ids

# =========================
# ✅ FUNÇÃO CORRIGIDA: PARSE DAS INFORMAÇÕES ADICIONAIS
# =========================
def parse_additional_infos(additional_infos: List[dict]) -> List[dict]:
    parsed_infos = []
    for info in additional_infos:
        options_list = []
        options_raw = info.get("options", {})
        
        if isinstance(options_raw, dict):
            for key, option_data in options_raw.items():
                if isinstance(option_data, dict):
                    options_list.append({
                        "id": option_data.get("id"),
                        "nome": option_data.get("name"),
                        "valor_adicional": safe_float(option_data.get("value"), 0.0),
                        "imagem_url": option_data.get("image", {}).get("https", "") if isinstance(option_data.get("image"), dict) else ""
                    })
        elif isinstance(options_raw, list):
            for option_data in options_raw:
                if isinstance(option_data, dict):
                    options_list.append({
                        "id": option_data.get("id"),
                        "nome": option_data.get("name"),
                        "valor_adicional": safe_float(option_data.get("value"), 0.0),
                        "imagem_url": option_data.get("image", {}).get("https", "") if isinstance(option_data.get("image"), dict) else ""
                    })
        
        parsed_info = {
            "id": info.get("id"),
            "info_id": info.get("info_id"),
            "nome": info.get("name"),
            "tipo": info.get("type"),
            "exibir_como": info.get("display_as"),
            "obrigatorio": info.get("required") == "1",
            "adicionar_ao_total": info.get("add_total") == "1",
            "ativo": info.get("active") == "1",
            "prazo_dias": safe_float(info.get("deadline"), 0),
            "opcoes": options_list
        }
        parsed_infos.append(parsed_info)
    return parsed_infos

# =========================
# 1) COLETA DO JSON DA EDIÇÃO
# =========================
def collect_product_data_destino(page: Page, produto_id: str, attempt: int = 1) -> Optional[dict]:
    product = {"produto_id": produto_id}
    detail_json = None
    
    def handle_response(response):
        nonlocal detail_json
        if detail_json:
            return
        try:
            ct = response.headers.get("content-type", "")
            if "application/json" not in ct:
                return
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                rid = data["data"].get("id")
                if rid is not None and str(rid) == str(produto_id):
                    detail_json = data["data"]
        except Exception:
            return
    
    page.on("response", handle_response)
    
    try:
        page.goto(
            f"https://www.grasielyatacado.com.br/admin/products/{produto_id}/edit",
            wait_until="domcontentloaded",
            timeout=CONFIG.timeout_per_product
        )
        
        waited = 0
        interval = 250
        while detail_json is None and waited < CONFIG.timeout_per_product:
            page.wait_for_timeout(interval)
            waited += interval
    except Exception:
        pass
    finally:
        page.remove_listener("response", handle_response)
    
    if not detail_json:
        if attempt < CONFIG.max_retries:
            page.wait_for_timeout(CONFIG.retry_delay)
            return collect_product_data_destino(page, produto_id, attempt + 1)
        return None
    
    try:
        d = detail_json
        seo_title = seo_description = None
        for tag in d.get("metatag", []):
            if tag.get("type") == "title":
                seo_title = tag.get("content")
            elif tag.get("type") == "description":
                seo_description = tag.get("content")
        
        images = d.get("ProductImage", [])
        first_image = images[0].get("https") if images else None
        
        url_obj = d.get("url", {})
        product_url = url_obj.get("https") if isinstance(url_obj, dict) else None
        
        additional_infos_raw = d.get("AdditionalInfos", [])
        additional_infos = parse_additional_infos(additional_infos_raw) if additional_infos_raw else []
        
        product.update({
            "nome": d.get("name"),
            "preco": safe_float(d.get("price")),
            "descricao": clean_html(d.get("description", "")),
            "estoque": d.get("stock"),
            "estoque_minimo": d.get("minimum_stock"),
            "categoria": d.get("category_name"),
            "referencia": d.get("reference"),
            "peso": d.get("weight"),
            "altura": d.get("height"),
            "largura": d.get("width"),
            "comprimento": d.get("length"),
            "imagem_url": first_image,
            "notificacao_estoque_baixo": d.get("minimum_stock_alert") == "1",
            "itens_inclusos": d.get("included_items"),
            "mensagem_adicional": d.get("additional_message"),
            "tempo_garantia": d.get("warranty"),
            "ativo": d.get("active") == "1",
            "visivel": d.get("visible") == "1",
            "informacoes_adicionais": additional_infos,
            "seo_preview": {
                "link": product_url,
                "title": seo_title,
                "description": seo_description
            }
        })
        return product
    except Exception:
        return None

# =========================
# 2) CAPTURA IDS - ✅ AGORA COM BUSCA DIRETA NO MODO TESTE
# =========================
def collect_all_product_ids_destino(page: Page, base_list_url: str) -> List[str]:
    all_ids = set()
    
    def is_list_response(response):
        try:
            url = response.url
            ct = response.headers.get("content-type", "")
            if response.status == 200 and "application/json" in ct:
                if "/api/products" in url and not re.search(r'/products/\d+', url):
                    return True
                if "products-search" in url or "/products/search" in url:
                    return True
            return False
        except:
            return False
    
    print("🔍 Iniciando coleta de IDs via interceptação de API...")
    
    # Primeira página (sempre executada)
    try:
        with page.expect_response(is_list_response, timeout=15000) as response_info:
            page.goto(base_list_url, wait_until="networkidle", timeout=30000)
        response = response_info.value
        data = response.json()
        if data.get("data"):
            page_ids = [str(item.get("id")) for item in data["data"] if item.get("id")]
            all_ids.update(page_ids)
            print(f"✓ Página 1: {len(page_ids)} produtos capturados")
    except Exception as e:
        print(f"⚠️ Erro ao capturar primeira página: {str(e)[:100]}")
        extract_ids_from_dom_destino(page, all_ids)
    
    # ✅ MODO TESTE: BUSCA DIRETA POR NOME (NOVA IMPLEMENTAÇÃO)
    if CONFIG.test_mode:
        print(f"\n🧪 MODO TESTE ATIVADO → Busca direta na barra de pesquisa")
        origem_names = load_origem_product_names()
        
        if origem_names:
            matched_ids = collect_matched_ids_via_search(page, origem_names)
            return matched_ids
        else:
            # fallback se não conseguir carregar origem
            all_ids_list = sorted(list(all_ids), key=lambda x: int(x) if x.isdigit() else 0)
            return all_ids_list[:CONFIG.test_limit]
    
    # MODO PRODUÇÃO: coleta completa com paginação
    print("📋 Modo produção: coletando TODOS os produtos (páginação completa)")
    current_page = 1
    no_progress_count = 0
    while current_page < CONFIG.max_pages and no_progress_count < CONFIG.max_scroll_attempts:
        previous_count = len(all_ids)
        next_clicked = try_click_next_page_destino(page, current_page, is_list_response, all_ids)
        
        if next_clicked:
            current_page += 1
            no_progress_count = 0
            continue
        
        extract_ids_from_dom_destino(page, all_ids)
        
        if len(all_ids) > previous_count:
            no_progress_count = 0
        else:
            no_progress_count += 1
        
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)
        except:
            pass
    
    all_ids_list = sorted(list(all_ids), key=lambda x: int(x) if x.isdigit() else 0)
    print(f"✅ Total de {len(all_ids_list)} IDs únicos capturados")
    return all_ids_list

def try_click_next_page_destino(page: Page, current_page: int, is_list_response, all_ids: set) -> bool:
    selectors = [
        "a.next:not(.disabled)", "button.next:not([disabled])",
        ".pagination a[rel='next']:not(.disabled)",
        "a[aria-label*='next']:not([aria-disabled='true'])",
        "button[aria-label*='next']:not([aria-disabled='true'])",
        "a:has-text('Próxima'):not(.disabled)", "a:has-text('Next'):not(.disabled)",
        "li.page-item:not(.disabled) a[aria-label='Next']",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            if locator.count() > 0:
                button = locator.first
                if button.is_visible():
                    print(f"➡️  Navegando para página {current_page + 1}...")
                    with page.expect_response(is_list_response, timeout=15000) as response_info:
                        button.click()
                    response = response_info.value
                    data = response.json()
                    if data.get("data"):
                        page_ids = [str(item.get("id")) for item in data["data"] if item.get("id")]
                        new_count = len([pid for pid in page_ids if pid not in all_ids])
                        all_ids.update(page_ids)
                        print(f"✓ Página {current_page + 1}: {len(page_ids)} produtos ({new_count} novos)")
                        return True
        except Exception:
            continue
    return False

def extract_ids_from_dom_destino(page: Page, all_ids: set) -> bool:
    try:
        ids_on_page = page.evaluate(r"""
            () => {
                const ids = new Set();
                document.querySelectorAll('a[href*="/products/"][href*="/edit"]').forEach(link => {
                    const match = link.href.match(/\/products\/(\d+)\/edit/);
                    if (match) ids.add(match[1]);
                });
                document.querySelectorAll('[data-id]').forEach(el => {
                    const id = el.getAttribute('data-id');
                    if (id && /^\d+$/.test(id)) ids.add(id);
                });
                document.querySelectorAll('[data-product-id]').forEach(el => {
                    const id = el.getAttribute('data-product-id');
                    if (id && /^\d+$/.test(id)) ids.add(id);
                });
                return Array.from(ids);
            }
        """)
        new_ids = [pid for pid in ids_on_page if pid not in all_ids]
        if new_ids:
            all_ids.update(new_ids)
            print(f"🔍 DOM: +{len(new_ids)} IDs encontrados")
            return True
    except Exception as e:
        print(f"⚠️ Erro ao extrair IDs do DOM: {str(e)[:50]}")
    return False

# =========================
# 3) PROCESSA PRODUTOS COM CHECKPOINT
# =========================
def process_all_products_destino(page: Page, product_ids: List[str], storage) -> List[dict]:
    tracker = ProgressTracker(len(product_ids))
    products = []
    buffer = []
    
    print(f"\n📦 Processando {len(product_ids)} produtos...")
    print(f"⚙️  Config OTIMIZADA: timeout={CONFIG.timeout_per_product}ms, retries={CONFIG.max_retries}, batch={CONFIG.batch_size}")
    if CONFIG.test_mode:
        print(f"🧪 MODO TESTE: Coletando apenas produtos encontrados na busca da ORIGEM")
    print(f"⚡ Tempo estimado: ~{len(product_ids) * CONFIG.timeout_per_product / 1000 / 60:.1f} minutos (melhor caso)")
    print()
    
    for idx, pid in enumerate(product_ids, 1):
        if CONFIG.test_mode or idx % 50 == 0:
            tracker._print_progress()
        
        try:
            product = collect_product_data_destino(page, pid)
            if product and product.get("nome"):
                products.append(product)
                buffer.append(product)
                tracker.log_success(pid, product.get("nome", ""))
                if len(buffer) >= CONFIG.batch_size:
                    save_batch(storage, buffer)
                    buffer = []
            else:
                tracker.log_failure(pid, "Sem dados após retries")
        except Exception as e:
            tracker.log_failure(pid, str(e))
    
    if buffer:
        save_batch(storage, buffer)
    
    tracker.print_summary()
    if tracker.failed_ids:
        save_failed_ids(tracker.failed_ids)
    
    return products

def save_batch(storage, products: List[dict]):
    try:
        if hasattr(storage, 'save_many'):
            storage.save_many(products)
        else:
            for product in products:
                try:
                    storage.save(product)
                except Exception as e:
                    print(f"⚠️ Erro ao salvar {product.get('produto_id')}: {str(e)[:40]}")
    except Exception as e:
        print(f"⚠️ Erro no salvamento em lote: {str(e)[:60]}")
        for product in products:
            try:
                storage.save(product)
            except:
                pass

def save_failed_ids(failed_ids: List[str]):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"failed_products_destino_{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                "timestamp": timestamp,
                "count": len(failed_ids),
                "ids": failed_ids
            }, f, indent=2, ensure_ascii=False)
        print(f"\n💾 IDs que falharam salvos em: {filename}")
    except Exception as e:
        print(f"⚠️ Não foi possível salvar lista de falhas: {str(e)}")

# =========================
# 4) FUNÇÃO PRINCIPAL
# =========================
def collect_all_products(page: Page, storage) -> List[dict]:
    base_list_url = (
        f"https://www.grasielyatacado.com.br/admin/products/list?"
        f"sort=name&page[size]={CONFIG.page_size}&page[number]=1"
    )
    
    print("\n" + "="*60)
    print("INICIANDO COLETA DE PRODUTOS DESTINO (ATACADO)")
    print("="*60)
    print(f"URL base: {base_list_url}")
    print(f"⚡ Timeout: {CONFIG.timeout_per_product}ms")
    print(f"⚡ Retries: {CONFIG.max_retries}")
    print(f"⚡ Batch: {CONFIG.batch_size}")
    if CONFIG.test_mode:
        print(f"🧪 MODO TESTE ATIVADO: Busca direta por nome da ORIGEM")
        print(f"   Para desativar: CONFIG.test_mode = False")
    print("="*60 + "\n")
    
    print("📋 ETAPA 1: COLETANDO IDS DOS PRODUTOS")
    print("-" * 60)
    product_ids = collect_all_product_ids_destino(page, base_list_url)
    
    if not product_ids:
        print("❌ Nenhum produto foi encontrado!")
        return []
    
    print("\n📦 ETAPA 2: COLETANDO DADOS DETALHADOS")
    print("-" * 60)
    all_products = process_all_products_destino(page, product_ids, storage)
    
    print("\n" + "="*60)
    print("✅ COLETA CONCLUÍDA")
    print("="*60)
    print(f"IDs encontrados: {len(product_ids)}")
    print(f"Produtos coletados: {len(all_products)}")
    print(f"Taxa de sucesso: {len(all_products)/len(product_ids)*100:.1f}%")
    print("="*60 + "\n")
    
    return all_products

# =========================
# 5) FUNÇÃO PARA REPROCESSAR FALHAS
# =========================
def retry_failed_products(page: Page, storage, failed_json_path: str) -> List[dict]:
    try:
        with open(failed_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        failed_ids = data.get("ids", [])
        print(f"🔄 Reprocessando {len(failed_ids)} produtos que falharam anteriormente...")
        return process_all_products_destino(page, failed_ids, storage)
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo de falhas: {str(e)}")
        return []