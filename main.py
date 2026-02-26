# main.py

import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from service.auth import authenticate
from service.scraper import collect_product_data

load_dotenv()

HEADLESS = False

ORIGEM_URL = os.getenv("ORIGEM_URL")
DESTINO_URL = os.getenv("DESTINO_URL")

SOURCE_USER = os.getenv("SOURCE_USER")
SOURCE_PASS = os.getenv("SOURCE_PASS")

TARGET_USER = os.getenv("TARGET_USER")
TARGET_PASS = os.getenv("TARGET_PASS")

COOKIES_ORIGEM_FILES = [
    "cookies_origem.json",
    "cookiesorigem.json",
    "cookies-origem.json"
]

COOKIES_DESTINO_FILES = [
    "cookies_destino.json",
    "cookiesdestino.json",
    "cookies-destino.json"
]


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()

        origem_page = authenticate(
            context,
            ORIGEM_URL,
            SOURCE_USER,
            SOURCE_PASS,
            COOKIES_ORIGEM_FILES
        )

        if not origem_page:
            print("Falha na autenticação da origem")
            browser.close()
            return

        print("Origem autenticada")

        # Executa scraper
        product_data = collect_product_data(origem_page)

        print("\n===== RESULTADO FINAL =====")
        for k, v in product_data.items():
            print(f"{k}: {v}")

        origem_page.close()
        browser.close()


if __name__ == "__main__":
    main()