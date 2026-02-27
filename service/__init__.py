# service/__init__.py
from .auth import authenticate, login_if_needed, load_cookies, save_cookies, human_type
from .scraper import collect_all_products, process_all_products
from .storage import storage  # instância compartilhada

__all__ = [
    'authenticate',
    'login_if_needed',
    'load_cookies',
    'save_cookies',
    'human_type',
    'collect_all_products',
    'process_all_products',
    'storage'
]