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
        # Só printa primeiros 20 erros para não poluir
        if len(self.failed_ids) <= 20:
            print(f"❌ [{self.current}/{self.total}] {pid} - {reason[:60]}")
    
    def log_retry(self, pid: str, attempt: int):
        self.retries += 1
        # Não printa todos os retries, só a cada 10
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
    """Remove HTML tags e retorna texto limpo"""
    if not html_text:
        return ""
    return BeautifulSoup(html_text, "html.parser").get_text(separator="\n").strip()

def safe_float(value, default=None) -> Optional[float]:
    """Converte string para float de forma segura"""
    if not value:
        return default
    try:
        return float(str(value).replace(",", "."))
    except (ValueError, AttributeError):
        return default

# =========================
# 1) COLETA DO JSON DA EDIÇÃO - OTIMIZADA
# =========================
def collect_product_data(page: Page, produto_id: str, attempt: int = 1) -> Optional[dict]:
    """
    Coleta dados de um produto com retry automático (OTIMIZADO)
    """
    product = {"produto_id": produto_id}
    detail_json = None
    
    def handle_response(response):
        nonlocal detail_json
        if detail_json:  # Já capturou
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
        except Exception:
            return
    
    page.on("response", handle_response)
    
    try:
        # Navega para página de edição
        page.goto(
            f"https://www.grasiely.com.br/admin/products/{produto_id}/edit",
            wait_until="domcontentloaded",  # OTIMIZADO: era networkidle
            timeout=CONFIG.timeout_per_product
        )
        
        # Espera pelo JSON com polling mais agressivo
        waited = 0
        interval = 250  # OTIMIZADO: era 300
        while detail_json is None and waited < CONFIG.timeout_per_product:
            page.wait_for_timeout(interval)
            waited += interval
            
    except Exception as e:
        # Só printa erro se for a última tentativa
        if attempt >= CONFIG.max_retries:
            pass  # Silencia para não poluir logs
    finally:
        page.remove_listener("response", handle_response)
    
    # Se não capturou e ainda tem tentativas, retry
    if not detail_json:
        if attempt < CONFIG.max_retries:
            page.wait_for_timeout(CONFIG.retry_delay)
            return collect_product_data(page, produto_id, attempt + 1)
        else:
            return None  # Falhou após todas as tentativas
    
    # Parse dos dados
    try:
        d = detail_json
        
        # Extrai informações do SEO
        seo_title = None
        seo_description = None
        metatags = d.get("metatag", [])
        for tag in metatags:
            if tag.get("type") == "title":
                seo_title = tag.get("content")
            elif tag.get("type") == "description":
                seo_description = tag.get("content")
        
        # Extrai primeira imagem
        images = d.get("ProductImage", [])
        first_image = images[0].get("https") if images and len(images) > 0 else None
        
        # URL do produto
        url_obj = d.get("url", {})
        product_url = url_obj.get("https") if isinstance(url_obj, dict) else None
        
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
            "seo_preview": {
                "link": product_url,
                "title": seo_title,
                "description": seo_description
            }
        })
        
        return product
        
    except Exception as e:
        return None

# =========================
# 2) CAPTURA IDS - PAGINAÇÃO ROBUSTA
# =========================
def collect_all_product_ids(page: Page, base_list_url: str) -> List[str]:
    """
    Coleta todos os IDs de produtos via interceptação de API e paginação
    """
    all_ids = set()
    captured_pages = []
    
    def is_list_response(response):
        """Identifica se a resposta é da listagem de produtos"""
        try:
            url = response.url
            return (
                ("products-search" in url or "/api/products" in url) and
                response.status == 200 and
                "application/json" in response.headers.get("content-type", "")
            )
        except:
            return False
    
    print("🔍 Iniciando coleta de IDs via interceptação de API...")
    
    # Primeira página
    try:
        with page.expect_response(is_list_response, timeout=10000) as response_info:
            page.goto(base_list_url, wait_until="networkidle", timeout=30000)
        
        response = response_info.value
        data = response.json()
        
        if data.get("data"):
            captured_pages.append(data)
            page_ids = [str(item.get("id")) for item in data["data"] if item.get("id")]
            all_ids.update(page_ids)
            print(f"✓ Página 1: {len(page_ids)} produtos")
    except Exception as e:
        print(f"⚠️ Erro ao capturar primeira página: {str(e)[:60]}")
        # Continua mesmo assim, pode conseguir via DOM
    
    # Paginação
    current_page = 1
    no_progress_count = 0
    
    while current_page < CONFIG.max_pages and no_progress_count < CONFIG.max_scroll_attempts:
        previous_count = len(all_ids)
        
        # Tenta encontrar e clicar no botão "próxima"
        next_clicked = try_click_next_page(page, current_page, is_list_response, all_ids)
        
        if next_clicked:
            current_page += 1
            no_progress_count = 0
            continue
        
        # Se não achou botão, tenta scraping do DOM
        new_ids_found = extract_ids_from_dom(page, all_ids)
        
        if len(all_ids) > previous_count:
            no_progress_count = 0
        else:
            no_progress_count += 1
            if no_progress_count % 5 == 0:  # Printa só a cada 5
                print(f"[INFO] Sem progresso ({no_progress_count}/{CONFIG.max_scroll_attempts})")
        
        # Tenta scroll para carregar mais
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)  # OTIMIZADO: era 1500
        except:
            pass
    
    all_ids_list = sorted(list(all_ids), key=lambda x: int(x) if x.isdigit() else 0)
    print(f"✅ Total de {len(all_ids_list)} IDs únicos capturados")
    
    return all_ids_list

def try_click_next_page(page: Page, current_page: int, is_list_response, all_ids: set) -> bool:
    """
    Tenta encontrar e clicar no botão de próxima página
    Retorna True se conseguiu clicar e capturar dados
    """
    selectors = [
        "a.next:not(.disabled)",
        "button.next:not([disabled])",
        ".pagination a[rel='next']:not(.disabled)",
        "a[aria-label*='next']:not([aria-disabled='true'])",
        "button[aria-label*='next']:not([aria-disabled='true'])",
        "a:has-text('Próxima'):not(.disabled)",
        "a:has-text('Next'):not(.disabled)",
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

def extract_ids_from_dom(page: Page, all_ids: set) -> bool:
    """
    Extrai IDs diretamente do DOM como fallback
    Retorna True se encontrou novos IDs
    """
    try:
        ids_on_page = page.evaluate("""
            () => {
                const ids = new Set();
                
                // Links de edição
                document.querySelectorAll('a[href*="/products/"][href*="/edit"]').forEach(link => {
                    const match = link.href.match(/\/products\/(\d+)\/edit/);
                    if (match) ids.add(match[1]);
                });
                
                // Atributos data-id
                document.querySelectorAll('[data-id]').forEach(el => {
                    const id = el.getAttribute('data-id');
                    if (id && /^\d+$/.test(id)) ids.add(id);
                });
                
                // Atributos data-product-id
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
            return True
            
    except Exception as e:
        print(f"⚠️ Erro ao extrair IDs do DOM: {str(e)[:50]}")
    
    return False

# =========================
# 3) PROCESSA PRODUTOS COM CHECKPOINT
# =========================
def process_all_products(page: Page, product_ids: List[str], storage) -> List[dict]:
    """
    Processa todos os produtos com tracking de progresso e salvamento em lote
    """
    tracker = ProgressTracker(len(product_ids))
    products = []
    buffer = []
    
    print(f"\n📦 Processando {len(product_ids)} produtos...")
    print(f"⚙️  Config OTIMIZADA: timeout={CONFIG.timeout_per_product}ms, retries={CONFIG.max_retries}, batch={CONFIG.batch_size}")
    print(f"⚡ Tempo estimado: ~{len(product_ids) * CONFIG.timeout_per_product / 1000 / 60 / 60:.1f} horas (melhor caso)")
    print()
    
    for idx, pid in enumerate(product_ids, 1):
        # Log de progresso a cada 50 produtos
        if idx % 50 == 0:
            tracker._print_progress()
        
        try:
            product = collect_product_data(page, pid)
            
            if product and product.get("nome"):
                products.append(product)
                buffer.append(product)
                tracker.log_success(pid, product.get("nome", ""))
                
                # Salva em lote
                if len(buffer) >= CONFIG.batch_size:
                    save_batch(storage, buffer)
                    buffer = []
            else:
                tracker.log_failure(pid, "Sem dados após retries")
                
        except Exception as e:
            tracker.log_failure(pid, str(e))
    
    # Salva resto do buffer
    if buffer:
        save_batch(storage, buffer)
    
    # Resumo final
    tracker.print_summary()
    
    # Salva lista de IDs que falharam
    if tracker.failed_ids:
        save_failed_ids(tracker.failed_ids)
    
    return products

def save_batch(storage, products: List[dict]):
    """Salva um lote de produtos, com fallback para salvamento individual"""
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
        # Tenta salvar individualmente
        for product in products:
            try:
                storage.save(product)
            except:
                pass

def save_failed_ids(failed_ids: List[str]):
    """Salva lista de IDs que falharam para reprocessamento posterior"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"failed_products_{timestamp}.json"
        
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
    """
    Função principal que orquestra toda a coleta (VERSÃO OTIMIZADA)
    """
    base_list_url = (
        f"https://www.grasiely.com.br/admin/products/list?"
        f"sort=name&page[size]={CONFIG.page_size}&page[number]=1"
    )
    
    print("\n" + "="*60)
    print("INICIANDO COLETA DE PRODUTOS (VERSÃO OTIMIZADA)")
    print("="*60)
    print(f"URL base: {base_list_url}")
    print(f"⚡ Timeout: {CONFIG.timeout_per_product}ms (era 20000ms)")
    print(f"⚡ Retries: {CONFIG.max_retries} (era 3)")
    print(f"⚡ Batch: {CONFIG.batch_size} (era 20)")
    print(f"🎯 Estimativa: ~40% mais rápido que versão anterior")
    print("="*60 + "\n")
    
    # ETAPA 1: Coletar IDs
    print("📋 ETAPA 1: COLETANDO IDS DOS PRODUTOS")
    print("-" * 60)
    product_ids = collect_all_product_ids(page, base_list_url)
    
    if not product_ids:
        print("❌ Nenhum produto foi encontrado!")
        return []
    
    # Descomente para testar com poucos produtos
    # product_ids = product_ids[:10]
    # print(f"⚠️  MODO TESTE: Processando apenas {len(product_ids)} produtos")
    
    # ETAPA 2: Coletar dados detalhados
    print("\n📦 ETAPA 2: COLETANDO DADOS DETALHADOS")
    print("-" * 60)
    all_products = process_all_products(page, product_ids, storage)
    
    # Resumo final
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
    """
    Reprocessa produtos que falharam na execução anterior
    """
    try:
        with open(failed_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        failed_ids = data.get("ids", [])
        print(f"🔄 Reprocessando {len(failed_ids)} produtos que falharam anteriormente...")
        
        return process_all_products(page, failed_ids, storage)
        
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo de falhas: {str(e)}")
        return []