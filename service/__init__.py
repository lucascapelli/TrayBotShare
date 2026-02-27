# service/__init__.py
"""
Service package - Integração de autenticação e scraping otimizado
"""

from .auth import (
    authenticate, 
    login_if_needed, 
    load_cookies, 
    save_cookies, 
    human_type,
    needs_login
)

# Tenta importar do scraper_optimized primeiro, senão do scraper normal
try:
    from .scraper import (
        collect_all_products,
        process_all_products,
        retry_failed_products,
        collect_all_product_ids,
        CONFIG as SCRAPER_CONFIG
    )
    print("✅ Usando scraper OTIMIZADO (6-7h)")
except ImportError:
    try:
        from .scraper import (
            collect_all_products,
            process_all_products,
            retry_failed_products,
            collect_all_product_ids,
            CONFIG as SCRAPER_CONFIG
        )
        print("⚠️ Usando scraper normal (11h)")
    except ImportError:
        print("❌ Erro: Nenhum scraper encontrado!")
        raise

# Importa storage
try:
    from .storage import storage
    __all__ = [
        # Auth
        'authenticate',
        'login_if_needed',
        'load_cookies',
        'save_cookies',
        'human_type',
        'needs_login',
        # Scraper
        'collect_all_products',
        'process_all_products',
        'retry_failed_products',
        'collect_all_product_ids',
        'SCRAPER_CONFIG',
        # Storage
        'storage'
    ]
except ImportError:
    __all__ = [
        # Auth
        'authenticate',
        'login_if_needed',
        'load_cookies',
        'save_cookies',
        'human_type',
        'needs_login',
        # Scraper
        'collect_all_products',
        'process_all_products',
        'retry_failed_products',
        'collect_all_product_ids',
        'SCRAPER_CONFIG'
    ]

# Configuração global
def get_config():
    """Retorna configuração atual do scraper"""
    return {
        'timeout_per_product': SCRAPER_CONFIG.timeout_per_product,
        'max_retries': SCRAPER_CONFIG.max_retries,
        'retry_delay': SCRAPER_CONFIG.retry_delay,
        'batch_size': SCRAPER_CONFIG.batch_size,
        'max_pages': SCRAPER_CONFIG.max_pages,
        'max_scroll_attempts': SCRAPER_CONFIG.max_scroll_attempts,
        'page_size': SCRAPER_CONFIG.page_size
    }

def set_config(**kwargs):
    """
    Atualiza configurações do scraper
    
    Exemplo:
        set_config(timeout_per_product=15000, max_retries=3)
    """
    for key, value in kwargs.items():
        if hasattr(SCRAPER_CONFIG, key):
            setattr(SCRAPER_CONFIG, key, value)
            print(f"✓ {key} = {value}")
        else:
            print(f"⚠️ Configuração '{key}' não existe")