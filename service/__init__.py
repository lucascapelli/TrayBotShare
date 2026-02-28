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

# Importa scrapers (origem e destino)
try:
    from .scraper import (
        collect_all_products as collect_origem,
        CONFIG as SCRAPER_CONFIG_ORIGEM
    )
    from .scraperDestino import (
        collect_all_products as collect_destino,
        CONFIG as SCRAPER_CONFIG_DESTINO
    )
    print("✅ Scrapers ORIGEM e DESTINO carregados (modo otimizado)")
except ImportError as e:
    print(f"❌ Erro ao carregar scrapers: {e}")
    raise

# Importa storage
try:
    from .storage import storage_origem, storage_destino
    __all__ = [
        # Auth
        'authenticate',
        'login_if_needed',
        'load_cookies',
        'save_cookies',
        'human_type',
        'needs_login',
        # Scrapers
        'collect_origem',
        'collect_destino',
        'SCRAPER_CONFIG_ORIGEM',
        'SCRAPER_CONFIG_DESTINO',
        # Storage
        'storage_origem',
        'storage_destino'
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
        # Scrapers
        'collect_origem',
        'collect_destino',
        'SCRAPER_CONFIG_ORIGEM',
        'SCRAPER_CONFIG_DESTINO'
    ]

# Configuração global
def get_config(origem=True):
    """Retorna configuração atual do scraper"""
    config = SCRAPER_CONFIG_ORIGEM if origem else SCRAPER_CONFIG_DESTINO
    return {
        'timeout_per_product': config.timeout_per_product,
        'max_retries': config.max_retries,
        'retry_delay': config.retry_delay,
        'batch_size': config.batch_size,
        'max_pages': config.max_pages,
        'max_scroll_attempts': config.max_scroll_attempts,
        'page_size': config.page_size,
        'test_mode': config.test_mode,
        'test_limit': config.test_limit
    }

def set_config(origem=True, **kwargs):
    """
    Atualiza configurações do scraper
    
    Exemplo:
        set_config(origem=True, test_mode=False)  # Produção na origem
        set_config(origem=False, test_limit=10)   # 10 produtos no destino
    """
    config = SCRAPER_CONFIG_ORIGEM if origem else SCRAPER_CONFIG_DESTINO
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
            print(f"✓ {'ORIGEM' if origem else 'DESTINO'}: {key} = {value}")
        else:
            print(f"⚠️ Configuração '{key}' não existe")