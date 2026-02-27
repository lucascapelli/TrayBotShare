# main.py
import os
import random
from dotenv import load_dotenv
from patchright.sync_api import sync_playwright
from service.auth import authenticate
from service.scraper import collect_all_products
from service.storage import storage

load_dotenv()

HEADLESS = True

ORIGEM_URL = os.getenv("ORIGEM_URL")
SOURCE_USER = os.getenv("SOURCE_USER")
SOURCE_PASS = os.getenv("SOURCE_PASS")

COOKIES_ORIGEM_FILES = [
    "cookies_origem.json",
    "cookiesorigem.json",
    "cookies-origem.json"
]


def main():
    print("=" * 60)
    print("🤖 TRAY BOT - SCRAPER DE PRODUTOS")
    print("=" * 60)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            channel="chrome",
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-dev-shm-usage"
            ]
        )

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo"
        )

        print("\n📋 ETAPA 1: AUTENTICAÇÃO")
        print("-" * 60)
        
        origem_page = authenticate(
            context,
            ORIGEM_URL,
            SOURCE_USER,
            SOURCE_PASS,
            COOKIES_ORIGEM_FILES
        )

        if not origem_page:
            print("\n❌ FALHA CRÍTICA: Não foi possível autenticar")
            browser.close()
            return

        print("\n📋 ETAPA 2: COLETA DE PRODUTOS")
        print("-" * 60)
        
        origin_products = collect_all_products(origem_page, storage)

        if len(origin_products) == 0:
            print("\n⚠️ AVISO: Nenhum produto foi coletado!")
            origem_page.close()
            browser.close()
            return

        print("\n" + "=" * 60)
        print("🎉 PROCESSO FINALIZADO")
        print(f"📦 Total de produtos coletados: {len(origin_products)}")
        print("=" * 60)
        
        # Exibe estatísticas
        stats = storage.get_statistics()
        print("\n📊 ESTATÍSTICAS:")
        print(f"  • Total de produtos: {stats['total']}")
        print(f"  • Com preço: {stats['com_preco']}")
        print(f"  • Com estoque: {stats['com_estoque']}")
        print(f"\n  Top 5 Categorias:")
        for cat, count in stats.get('top_5_categorias', []):
            print(f"    - {cat}: {count} produtos")
        
        print(f"\n💾 Dados salvos em: produtos/ProdutosOrigem.json")
        
        # Exportar CSV
        try:
            storage.export_csv("produtos/ProdutosOrigem.csv")
            print(f"📊 CSV exportado para: produtos/ProdutosOrigem.csv")
        except Exception as e:
            print(f"⚠️ Erro ao exportar CSV: {e}")

        origem_page.close()
        browser.close()


if __name__ == "__main__":
    main()