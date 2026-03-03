from typing import List, Optional, Tuple

from patchright.sync_api import Page

from .config import DESTINO_BASE


def find_one_match(
    page: Page,
    origem_products: List[dict],
    human_delay,
    short_delay,
    logger,
) -> Optional[dict]:
    base_url = f"{DESTINO_BASE}/admin/products/list?sort=name&page[size]=25&page[number]=1"

    try:
        page.goto(base_url, wait_until="networkidle", timeout=15000)
    except Exception as exc:
        logger.error("❌ Erro ao carregar listagem: %s", exc)
        return None

    search_box = page.get_by_role("textbox", name="Buscar por nome, código,")

    for idx, produto in enumerate(origem_products, 1):
        nome = (produto.get("nome") or "").strip()
        if not nome:
            continue

        logger.info("[%d/%d] Buscando: '%s'", idx, len(origem_products), nome[:60])

        try:
            search_box.clear()
            search_box.fill(nome)

            with page.expect_response(
                lambda response: (
                    response.status == 200
                    and "application/json" in (response.headers.get("content-type", "") or "")
                    and any(x in response.url for x in ["/api/products", "products-search", "/products/search"])
                ),
                timeout=10000,
            ) as resp_info:
                page.keyboard.press("Enter")

            data = resp_info.value.json()

            for item in (data.get("data") or []):
                product_id = str(item.get("id", ""))
                item_name = (item.get("name") or "").strip()
                if product_id and (nome.lower() in item_name.lower() or item_name.lower() in nome.lower()):
                    logger.info("✅ Match encontrado: '%s' → ID %s", nome[:50], product_id)
                    return {
                        "destino_id": product_id,
                        "destino_name": item_name,
                        "origem_product": produto,
                    }

            human_delay(2.0, 4.0)

        except Exception as exc:
            logger.debug("[%d] Busca falhou: %s", idx, exc)
            short_delay()
            continue

    logger.warning("❌ Nenhum match encontrado")
    return None


def fetch_product_and_token(page: Page, product_id: str, logger) -> Tuple[Optional[dict], Optional[str]]:
    detail_json = None
    auth_token = None

    def _on_response(response):
        nonlocal detail_json, auth_token
        if detail_json:
            return

        try:
            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                return
            body = response.json()
            if isinstance(body, dict) and "data" in body:
                response_id = body["data"].get("id")
                if response_id is not None and str(response_id) == str(product_id):
                    detail_json = body["data"]
                    token = response.request.headers.get("authorization")
                    if token:
                        auth_token = token
        except Exception:
            pass

    page.on("response", _on_response)
    try:
        page.goto(
            f"{DESTINO_BASE}/admin/products/{product_id}/edit",
            wait_until="domcontentloaded",
            timeout=15000,
        )
        waited = 0
        while detail_json is None and waited < 12000:
            page.wait_for_timeout(300)
            waited += 300
    except Exception as exc:
        logger.warning("Erro ao carregar produto %s: %s", product_id, exc)
    finally:
        page.remove_listener("response", _on_response)

    if not auth_token:
        try:
            auth_token = page.evaluate("""(() => {
                const keys = ['token','access_token','auth_token','authorization','jwt','bearer','api_token'];
                for (const k of keys) { const v = localStorage.getItem(k); if (v && v.length > 10) return v; }
                for (let i = 0; i < localStorage.length; i++) {
                    const k = localStorage.key(i); const v = localStorage.getItem(k);
                    if (v && v.startsWith('eyJ')) return v;
                }
                return null;
            })()""")
        except Exception:
            pass

    if auth_token and not auth_token.lower().startswith("bearer "):
        auth_token = f"Bearer {auth_token}"

    if auth_token:
        try:
            resp = page.request.get(
                f"{DESTINO_BASE}/admin/api/products/{product_id}",
                headers={
                    "Accept": "application/json",
                    "Authorization": auth_token,
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{DESTINO_BASE}/admin/products/{product_id}/edit",
                },
            )
            if resp.status == 200:
                payload = resp.json()
                if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                    detail_json = payload.get("data")
                    logger.info("📥 JSON do produto %s capturado via GET /admin/api/products/{id}", product_id)
            else:
                logger.warning("GET /admin/api/products/%s retornou status %d", product_id, resp.status)
        except Exception as exc:
            logger.warning("Erro no GET /admin/api/products/%s: %s", product_id, exc)

    if detail_json:
        logger.info("📄 JSON do produto %s capturado", product_id)
    else:
        logger.error("❌ Não conseguiu capturar JSON do produto %s", product_id)

    if auth_token:
        logger.info("🔑 Token capturado")
    else:
        logger.warning("⚠️ Token NÃO capturado")

    return detail_json, auth_token
