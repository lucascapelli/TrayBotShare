# main.py - VERSÃO ATUALIZADA COM MENU + SUPORTE DESTINO + PREP PARA SYNC
import os
import json
from dotenv import load_dotenv
from patchright.sync_api import sync_playwright

from service.auth import authenticate
from service.scraper import collect_all_products
from service.storage import JSONStorage
# NOVO: import do sync (vamos criar esse arquivo)
from service.sync import run_sync

load_dotenv()

HEADLESS = False 

# ==================== CONFIGURAÇÃO GERAL ====================
ORIGEM_URL = os.getenv("ORIGEM_URL")
DESTINO_URL = os.getenv("DESTINO_URL") or "https://www.grasielyatacado.com.br/admin/products/list"

SOURCE_USER = os.getenv("SOURCE_USER")
SOURCE_PASS = os.getenv("SOURCE_PASS")
TARGET_USER = os.getenv("TARGET_USER")
TARGET_PASS = os.getenv("TARGET_PASS")

# Cookies separados para não misturar sessões
COOKIES_ORIGEM_FILES = ["cookies_origem.json", "cookiesorigem.json", "cookies-origem.json"]
COOKIES_DESTINO_FILES = ["cookies_destino.json", "cookiesdestino.json", "cookies-destino.json"]

# Storages separados (agora instanciamos com paths diferentes)
STORAGE_ORIGEM = JSONStorage(
    json_path="produtos/ProdutosOrigem.json",
    csv_path="produtos/ProdutosOrigem.csv"
)
STORAGE_DESTINO = JSONStorage(
    json_path="produtos/ProdutosDestino.json",
    csv_path="produtos/ProdutosDestino.csv"
)


def main():
    print("=" * 70)
    print("🤖 TRAY BOT - ORIGEM ↔ DESTINO (Grasiely + Atacado)")
    print("=" * 70)
    print("1️⃣  Colher dados ORIGEM")
    print("2️⃣  Colher dados DESTINO")
    print("3️⃣  Comparar + Escrever na ORIGEM (sync destino → origem)")
    print("0️⃣  Sair")
    print("-" * 70)

    escolha = input("Escolha uma opção (1/2/3/0): ").strip()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            channel="chrome",
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo"
        )

        if escolha == "1":
            print("\n📋 COLETANDO ORIGEM...")
            page = authenticate(context, ORIGEM_URL, SOURCE_USER, SOURCE_PASS, COOKIES_ORIGEM_FILES)
            if page:
                collect_all_products(page, STORAGE_ORIGEM)   # agora passa storage específico

        elif escolha == "2":
            print("\n📋 COLETANDO DESTINO...")
            page = authenticate(context, DESTINO_URL, TARGET_USER, TARGET_PASS, COOKIES_DESTINO_FILES)
            if page:
                collect_all_products(page, STORAGE_DESTINO)

        elif escolha == "3":
            print("\n🔄 INICIANDO COMPARAÇÃO + SYNC (destino → origem)...")
            run_sync(context, STORAGE_ORIGEM, STORAGE_DESTINO, ORIGEM_URL, SOURCE_USER, SOURCE_PASS, COOKIES_ORIGEM_FILES)
            # o sync abre o navegador sozinho se precisar

        elif escolha == "0":
            print("👋 Até mais!")
            browser.close()
            return
        else:
            print("❌ Opção inválida")

        browser.close()


if __name__ == "__main__":
    main()