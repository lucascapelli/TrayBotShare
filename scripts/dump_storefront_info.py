import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from patchright.sync_api import sync_playwright

BASE = "https://www.grasielyatacado.com.br"
LOJA_ID = "1416180"
PRODUCT_IDS = ["47"]


def _product_page_url(product_id: str) -> str:
    return f"{BASE}/loja/produto.php?loja={LOJA_ID}&IdProd={product_id}&iniSession=1"


def _info_url() -> str:
    return f"{BASE}/nocache/info.php?loja={LOJA_ID}"


def _extract_summary(payload: Any) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "payload_type": type(payload).__name__,
    }

    if isinstance(payload, dict):
        summary["top_keys"] = sorted(payload.keys())[:30]
        list_keys = [k for k, v in payload.items() if isinstance(v, list)]
        summary["list_keys"] = list_keys
        summary["list_sizes"] = {k: len(payload[k]) for k in list_keys[:10]}
    elif isinstance(payload, list):
        summary["list_len"] = len(payload)
        if payload and isinstance(payload[0], dict):
            summary["first_item_keys"] = sorted(payload[0].keys())[:30]

    return summary


def _summarize_additional_data(additional_data: Any) -> Dict[str, Any]:
    if not isinstance(additional_data, dict):
        return {"available": False}

    info_ids: List[str] = []
    info_names: List[str] = []
    option_count = 0

    for value in additional_data.values():
        if not isinstance(value, dict):
            continue
        info_id = value.get("id_informacoes_produto_cadastro")
        info_name = value.get("nome") or value.get("nome_exibicao_adm")
        if info_id:
            info_ids.append(str(info_id))
        if info_name:
            info_names.append(str(info_name))

        options = value.get("opcoes")
        if isinstance(options, dict):
            option_count += len(options)

    return {
        "available": True,
        "infos_count": len(info_ids),
        "info_ids": info_ids,
        "info_names": info_names,
        "total_options": option_count,
    }


def _extract_additional_from_html(html: str) -> Dict[str, Any]:
    if not html:
        return {"params": None, "dados": None}

    params = None
    dados = None

    params_match = re.search(r"InformacaoAdicional\.params\s*=\s*(\{.*?\});", html, re.DOTALL)
    if params_match:
        try:
            params = json.loads(params_match.group(1))
        except Exception:
            params = None

    dados_match = re.search(r"InformacaoAdicional\.dados\s*=\s*(\{.*?\});", html, re.DOTALL)
    if dados_match:
        try:
            dados = json.loads(dados_match.group(1))
        except Exception:
            dados = None

    return {"params": params, "dados": dados}


def run() -> str:
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)

    report: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "base": BASE,
        "loja": LOJA_ID,
        "products": {},
    }

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel="chrome", args=["--no-sandbox"])
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
        )
        page = context.new_page()

        try:
            for product_id in PRODUCT_IDS:
                item: Dict[str, Any] = {
                    "product_id": product_id,
                    "product_url": _product_page_url(product_id),
                    "info_url": _info_url(),
                }

                try:
                    page.goto(item["product_url"], wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(800)
                    referer = page.url
                    item["resolved_referer"] = referer

                    try:
                        additional_params = page.evaluate(
                            """
                            () => {
                                if (typeof InformacaoAdicional === 'undefined') {
                                    return null;
                                }
                                return {
                                    params: InformacaoAdicional.params || null,
                                    dados: InformacaoAdicional.dados || null,
                                };
                            }
                            """
                        )
                    except Exception:
                        additional_params = None

                    if isinstance(additional_params, dict):
                        item["informacao_adicional"] = {
                            "params": additional_params.get("params"),
                            "dados_summary": _summarize_additional_data(additional_params.get("dados")),
                            "dados": additional_params.get("dados"),
                        }

                    if (
                        "informacao_adicional" not in item
                        or not item["informacao_adicional"].get("dados")
                    ):
                        html = page.content()
                        html_data = _extract_additional_from_html(html)
                        item["informacao_adicional"] = {
                            "params": html_data.get("params"),
                            "dados_summary": _summarize_additional_data(html_data.get("dados")),
                            "dados": html_data.get("dados"),
                            "source": "html_regex_fallback",
                        }

                    headers = {
                        "accept": "*/*",
                        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                        "referer": referer,
                        "x-requested-with": "XMLHttpRequest",
                    }

                    response = page.request.get(item["info_url"], headers=headers)
                    item["http_status"] = response.status
                    item["ok"] = response.ok
                    item["response_content_type"] = response.headers.get("content-type")

                    try:
                        payload = response.json()
                        item["summary"] = _extract_summary(payload)
                        item["json"] = payload
                    except Exception:
                        text = response.text()
                        item["text_snippet"] = text[:1200]
                        item["summary"] = {
                            "payload_type": "text",
                            "text_len": len(text),
                        }
                except Exception as exc:
                    item["error"] = str(exc)

                report["products"][product_id] = item

        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(ROOT, "logs", f"storefront_info_{LOJA_ID}_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)

    print(out_path)
    return out_path


if __name__ == "__main__":
    run()
