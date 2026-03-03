import json
import os
import random
import sys
import time
from typing import Optional

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from patchright.sync_api import sync_playwright

from service.auth import load_storage_state, _resolve_state_path
from service.sync_mod.destino_page import fetch_product_and_token
from service.sync_mod.services.additional_info_sync import sync_additional_infos

PRODUCT_IDS = ["47", "53"]
ORIGEM_JSON_PATH = os.path.join(ROOT, "produtos", "ProdutosOrigem.json")
DESTINO_BASE = "https://www.grasielyatacado.com.br"
DESTINO_BOOTSTRAP_URL = f"{DESTINO_BASE}/admin/products/list"
COOKIES_DESTINO = ["cookies_destino.json", "cookiesdestino.json", "cookies-destino.json"]


def _human_delay(min_s: float = 1.0, max_s: float = 2.5):
    mid = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mid, std)))
    time.sleep(delay)


def _short_delay():
    _human_delay(0.8, 1.6)


def _medium_delay():
    _human_delay(1.6, 2.8)


def _load_origem_products() -> list:
    with open(ORIGEM_JSON_PATH, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        return []
    return data


def _find_origem_product(produtos: list, product_id: str) -> Optional[dict]:
    pid = str(product_id)
    for item in produtos:
        if str(item.get("produto_id", "")) == pid:
            return item
    return None


def _build_destino_context(browser):
    state_path = _resolve_state_path(COOKIES_DESTINO)
    stored = load_storage_state(state_path)

    kwargs = {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "pt-BR",
        "timezone_id": "America/Sao_Paulo",
    }
    if stored:
        kwargs["storage_state"] = stored

    return browser.new_context(**kwargs)


def _save_report(report: dict) -> str:
    logs_dir = os.path.join(ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(logs_dir, f"sync_additional_infos_47_53_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)
    return out_path


def run() -> str:
    produtos_origem = _load_origem_products()

    report = {
        "product_ids": PRODUCT_IDS,
        "results": {},
    }

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, channel="chrome", args=["--no-sandbox"])
        context = _build_destino_context(browser)
        page = context.new_page()

        try:
            page.goto(DESTINO_BOOTSTRAP_URL, wait_until="domcontentloaded", timeout=20000)

            for pid in PRODUCT_IDS:
                entry = {"product_id": pid}
                origem_prod = _find_origem_product(produtos_origem, pid)

                if not origem_prod:
                    entry["status"] = "origem_nao_encontrado"
                    report["results"][pid] = entry
                    continue

                destino_json, token = fetch_product_and_token(page, pid, logger=type("L", (), {
                    "info": staticmethod(lambda *a, **k: None),
                    "warning": staticmethod(lambda *a, **k: None),
                    "error": staticmethod(lambda *a, **k: None),
                    "debug": staticmethod(lambda *a, **k: None),
                })())

                if not destino_json:
                    entry["status"] = "destino_json_nao_capturado"
                    report["results"][pid] = entry
                    continue

                if not token:
                    entry["status"] = "token_nao_capturado"
                    report["results"][pid] = entry
                    continue

                log_entry = {}
                sync_additional_infos(
                    page,
                    pid,
                    origem_prod.get("informacoes_adicionais", []),
                    token,
                    log_entry,
                    short_delay=_short_delay,
                    medium_delay=_medium_delay,
                )

                entry["status"] = "ok"
                entry["infos_adicionais"] = log_entry.get("infos_adicionais", {})
                report["results"][pid] = entry

        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    out_path = _save_report(report)
    print(out_path)
    return out_path


if __name__ == "__main__":
    run()
