from patchright.sync_api import sync_playwright
import main
from service.fix_produto_47 import run_fix_produto
import json
import os
from urllib.parse import urlparse

PRODUCT_ID = "61"
ORIGEM_BACKUP = "produtos/ProdutosOrigem.json.bak"
ORIGEM_PATH = "produtos/ProdutosOrigem.json"


def save_origin_product_to_storage(prod_data: dict):
    try:
        if os.path.isfile(ORIGEM_PATH):
            if not os.path.isfile(ORIGEM_BACKUP):
                os.rename(ORIGEM_PATH, ORIGEM_BACKUP)
            else:
                os.remove(ORIGEM_BACKUP)
                os.rename(ORIGEM_PATH, ORIGEM_BACKUP)
    except Exception:
        pass

    # Salva apenas o conteúdo útil (data), sem lista extra
    data_to_save = prod_data.get("data") if isinstance(prod_data, dict) and "data" in prod_data else prod_data

    try:
        with open(ORIGEM_PATH, "w", encoding="utf-8") as f:
            json.dump(data_to_save, f, indent=2, ensure_ascii=False)
        print(f"✅ Gravado produto {PRODUCT_ID} em {ORIGEM_PATH} (backup em {ORIGEM_BACKUP})")
    except Exception as e:
        print("Erro gravando origem:", e)


if __name__ == "__main__":
    main._install_asyncio_exception_filter()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=main.HEADLESS,
            channel="chrome",
            args=["--no-sandbox"],
        )
        try:
            # ORIGEM - autenticação e captura real
            ctx_o, page_o = main.auth_in_context(
                browser, main.ORIGEM_URL, main.SOURCE_USER, main.SOURCE_PASS, main.COOKIES_ORIGEM, "ORIGEM"
            )
            origem_prod_data = None
            if page_o:
                origin_base = main.ORIGEM_URL or ""
                p = urlparse(origin_base)
                origin_base = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else origin_base

                headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
                token = None
                try:
                    from service.sync_mod.destino_api import _extract_origin_token
                    token = _extract_origin_token(page_o)
                    if token:
                        headers["Authorization"] = token
                except Exception:
                    pass

                # GET produto completo
                url_prod = f"{origin_base}/admin/api/products/{PRODUCT_ID}"
                resp = page_o.request.get(url_prod, headers=headers, timeout=45000)
                print("ORIGEM GET status:", resp.status)

                if resp.status == 200:
                    body = resp.json()
                    origem_prod_data = body.get("data") or body

                    # GET variações completas (prioridade alta)
                    variants_url = (
                        f"{origin_base}/admin/api/products-variants"
                        f"?filter[product_id]={PRODUCT_ID}&page[size]=100&sort=order"
                    )
                    vresp = page_o.request.get(variants_url, headers=headers, timeout=30000)
                    print("ORIGEM variants status:", vresp.status)

                    if vresp.status == 200:
                        vbody = vresp.json()
                        variants = vbody.get("data") or []
                        if variants:
                            origem_prod_data["variacoes"] = variants  # chave que o domain espera
                            origem_prod_data["Variant"] = [ {"id": v["id"]} for v in variants ]
                            print(f"✅ Anexadas {len(variants)} variações completas ao produto origem")

                    save_origin_product_to_storage(origem_prod_data)
                else:
                    print("Falha ao obter produto da ORIGEM, status:", resp.status)

            # DESTINO - autenticação e repair
            ctx, page = main.auth_in_context(
                browser, main.DESTINO_URL, main.TARGET_USER, main.TARGET_PASS, main.COOKIES_DESTINO, "DESTINO"
            )
            if not page:
                print("❌ Autenticação no DESTINO falhou")
            else:
                run_fix_produto(ctx, PRODUCT_ID)

        finally:
            try:
                browser.close()
            except Exception:
                pass