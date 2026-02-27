# main.py
import os
import random
from dotenv import load_dotenv
from patchright.sync_api import sync_playwright  # ← TROQUEI AQUI
from service.auth import authenticate
from service.scraper import collect_all_products
from service import storage

load_dotenv()

HEADLESS = True   # ← pode voltar para True agora

ORIGEM_URL = os.getenv("ORIGEM_URL")
SOURCE_USER = os.getenv("SOURCE_USER")
SOURCE_PASS = os.getenv("SOURCE_PASS")

COOKIES_ORIGEM_FILES = [
    "cookies_origem.json",
    "cookiesorigem.json",
    "cookies-origem.json"
]


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            channel="chrome",   # usa Chrome real (mais stealth)
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

        origem_page = authenticate(
            context,
            ORIGEM_URL,
            SOURCE_USER,
            SOURCE_PASS,
            COOKIES_ORIGEM_FILES
        )

        if not origem_page:
            print("❌ Falha na autenticação da origem")
            browser.close()
            return

        print("✅ Origem autenticada com sucesso!")

        origin_products = collect_all_products(origem_page, storage)

        print(f"\nTotal coletado: {len(origin_products)} produtos")
        print("\n===== RESULTADO FINAL =====")

        for product in origin_products:
            print("---------------")
            for k, v in product.items():
                print(f"{k}: {v}")

        # FECHA SÓ NO FINAL (bug corrigido)
        origem_page.close()
        browser.close()


if __name__ == "__main__":
    main()