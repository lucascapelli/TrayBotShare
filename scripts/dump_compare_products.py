import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from patchright.sync_api import sync_playwright
from service.auth import load_storage_state, _resolve_state_path

load_dotenv()

TARGETS = {
    "origem": {
        "base": "https://www.grasiely.com.br",
        "bootstrap_url": os.getenv("ORIGEM_URL", "https://www.grasiely.com.br/admin/products/list"),
        "auth_token_env": "ORIGEM_AUTH_TOKEN",
        "cookie_files": ["cookies_origem.json", "cookiesorigem.json", "cookies-origem.json"],
    },
    "destino": {
        "base": "https://www.grasielyatacado.com.br",
        "bootstrap_url": os.getenv("DESTINO_URL", "https://www.grasielyatacado.com.br/admin/products/list"),
        "auth_token_env": "DESTINO_AUTH_TOKEN",
        "cookie_files": ["cookies_destino.json", "cookiesdestino.json", "cookies-destino.json"],
    },
}

PRODUCT_IDS = ["47", "53"]

# Notas operacionais dos casos já validados:
# - 47: produto conhecido como corrompido no destino; manter fora do fluxo automático de sync.
# - 53: produto usa infos adicionais como personalização; pode não expor variações via API (Variant=[]).
PRODUCT_NOTES = {
    "47": "corrompido_no_destino_skip_sync",
    "53": "usa_infos_adicionais_pode_nao_ter_variant_api",
}


def _classify_model(data: Dict[str, Any]) -> str:
    has_variation = str(data.get("has_variation", ""))
    variant_len = len(data.get("Variant") or [])
    properties_len = len(data.get("Properties") or [])
    additional_infos_len = len(data.get("AdditionalInfos") or [])

    if has_variation == "1" or variant_len > 0 or properties_len > 0:
        return "variantes_api"
    if additional_infos_len > 0:
        return "infos_adicionais"
    return "sem_personalizacao"


def _extract_token_from_storage(page) -> Optional[str]:
    token = None
    keys = ["token", "access_token", "auth_token", "authorization", "jwt", "bearer", "api_token"]
    for key in keys:
        try:
            value = page.evaluate("(k) => localStorage.getItem(k)", key)
            if value and isinstance(value, str) and len(value) > 10:
                token = value
                break
        except Exception:
            pass

    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return token


def _capture_token_from_edit(page, base: str, product_id: str) -> Optional[str]:
    captured = None

    def on_request(request):
        nonlocal captured
        if captured:
            return
        try:
            url = request.url or ""
            if "/admin/api/" not in url:
                return
            token = request.headers.get("authorization")
            if token:
                captured = token
        except Exception:
            pass

    def on_response(response):
        nonlocal captured
        if captured:
            return
        try:
            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                return
            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                token = response.request.headers.get("authorization")
                if token:
                    captured = token
        except Exception:
            pass

    page.on("request", on_request)
    page.on("response", on_response)
    try:
        page.goto(f"{base}/admin/products/{product_id}/edit", wait_until="networkidle", timeout=30000)
        waited = 0
        while captured is None and waited < 10000:
            page.wait_for_timeout(300)
            waited += 300
    except Exception:
        pass
    finally:
        page.remove_listener("request", on_request)
        page.remove_listener("response", on_response)

    if captured and not captured.lower().startswith("bearer "):
        captured = f"Bearer {captured}"
    return captured


def _fetch_product(page, base: str, product_id: str, token: Optional[str]) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{base}/admin/products/{product_id}/edit",
    }
    if token:
        headers["Authorization"] = token
    else:
        try:
            cookies = page.context.cookies(base)
            if cookies:
                cookie_header = "; ".join(f"{c.get('name')}={c.get('value')}" for c in cookies if c.get("name") and c.get("value") is not None)
                if cookie_header:
                    headers["Cookie"] = cookie_header
        except Exception:
            pass

    url = f"{base}/admin/api/products/{product_id}"
    resp = page.request.get(url, headers=headers)

    result = {
        "url": url,
        "http_status": resp.status,
        "ok": resp.ok,
        "has_token": bool(token),
    }

    try:
        payload = resp.json()
        result["json"] = payload
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            data = payload["data"]
            result["summary"] = {
                "id": data.get("id"),
                "name": data.get("name"),
                "price": data.get("price"),
                "stock": data.get("stock"),
                "has_variation": data.get("has_variation"),
                "additional_infos_len": len(data.get("AdditionalInfos") or []),
                "variant_len": len(data.get("Variant") or []),
                "properties_len": len(data.get("Properties") or []),
                "product_image_len": len(data.get("ProductImage") or []),
                "modelo_personalizacao": _classify_model(data),
            }
            if product_id in PRODUCT_NOTES:
                result["case_note"] = PRODUCT_NOTES[product_id]
    except Exception:
        try:
            result["text_snippet"] = resp.text()[:600]
        except Exception:
            result["text_snippet"] = "<no body>"

    return result


def _build_context_with_state(browser, cookie_files):
    state_path = _resolve_state_path(cookie_files)
    state_data = load_storage_state(state_path)
    kwargs = {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "pt-BR",
        "timezone_id": "America/Sao_Paulo",
    }
    if state_data:
        kwargs["storage_state"] = state_data
    return browser.new_context(**kwargs), state_path


def run() -> str:
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)

    report = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "products": PRODUCT_IDS,
        "sources": {},
    }

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel="chrome", args=["--no-sandbox"])
        try:
            for source_name, source in TARGETS.items():
                source_report = {
                    "base": source["base"],
                    "state_file": _resolve_state_path(source["cookie_files"]),
                    "state_exists": os.path.isfile(_resolve_state_path(source["cookie_files"])),
                    "auth_ok": False,
                    "token_found": False,
                    "items": {},
                }

                context, _state_path = _build_context_with_state(browser, source["cookie_files"])
                page = context.new_page()

                try:
                    try:
                        page.goto(source["bootstrap_url"], wait_until="domcontentloaded", timeout=20000)
                    except Exception as exc:
                        source_report["bootstrap_error"] = str(exc)

                    try:
                        current_url = (page.url or "").lower()
                        source_report["auth_ok"] = ("/admin/" in current_url and "login" not in current_url)
                    except Exception:
                        source_report["auth_ok"] = False

                    token = os.getenv(source.get("auth_token_env", ""), "").strip() or None
                    if token and not token.lower().startswith("bearer "):
                        token = f"Bearer {token}"

                    if token:
                        source_report["token_source"] = "env"

                    if not token:
                        token = _extract_token_from_storage(page)
                    if not token:
                        token = _capture_token_from_edit(page, source["base"], PRODUCT_IDS[0])
                    if token and "token_source" not in source_report:
                        source_report["token_source"] = "session"

                    source_report["token_found"] = bool(token)

                    for product_id in PRODUCT_IDS:
                        item_result = _fetch_product(page, source["base"], product_id, token)

                        if item_result.get("http_status") == 401:
                            refreshed = _capture_token_from_edit(page, source["base"], product_id)
                            if refreshed:
                                token = refreshed
                                source_report["token_refreshed_on_401"] = True
                                item_result = _fetch_product(page, source["base"], product_id, token)

                        source_report["items"][product_id] = item_result

                except Exception as exc:
                    source_report["error"] = str(exc)
                finally:
                    try:
                        context.close()
                    except Exception as exc:
                        source_report["context_close_error"] = str(exc)

                report["sources"][source_name] = source_report
        finally:
            try:
                browser.close()
            except Exception:
                pass

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(ROOT, "logs", f"compare_origem_destino_47_53_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)

    print(out_path)
    return out_path


if __name__ == "__main__":
    run()
