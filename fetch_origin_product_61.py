# fetch_origin_product_61.py
from patchright.sync_api import sync_playwright
from urllib.parse import urlparse
import json
import main

PRODUCT_ID = "61"
OUT_PATH = "produtos/product_61_response.json"

if __name__ == "__main__":
    main._install_asyncio_exception_filter()
    parsed = urlparse(main.ORIGEM_URL or "")
    origin_base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else (main.ORIGEM_URL or "")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=main.HEADLESS, channel="chrome", args=["--no-sandbox"]) 
        try:
            ctx, page = main.auth_in_context(browser, main.ORIGEM_URL, main.SOURCE_USER, main.SOURCE_PASS, main.COOKIES_ORIGEM, "ORIGEM")
            if not page:
                print("❌ Autenticação na ORIGEM falhou — verifique cookies/credenciais")
            else:
                url = f"{origin_base}/admin/api/products/{PRODUCT_ID}"
                headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
                try:
                    resp = page.request.get(url, headers=headers, timeout=45000)
                    print("HTTP status:", resp.status)
                    try:
                        data = resp.json()
                    except Exception as e:
                        print("Erro parse JSON:", e)
                        data = None
                    if resp.status == 200 and data:
                        with open(OUT_PATH, "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=2, ensure_ascii=False)
                        print(f"✅ Salvo {OUT_PATH}")
                    else:
                        print("⚠️ Requisição não retornou 200 ou body vazio. status=", resp.status)
                        if data:
                            print("Resposta: ", json.dumps(data, indent=2, ensure_ascii=False)[:1000])
                except Exception as e:
                    print("Erro na request:", e)
        finally:
            try:
                browser.close()
            except Exception:
                pass
