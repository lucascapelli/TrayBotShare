import json
import random
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from patchright.sync_api import Page

from .config import DESTINO_BASE
from .domain import api_headers, normalize


def get_product_details(page: Page, product_id: str, token: str, logger) -> Optional[dict]:
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}"
    try:
        resp = page.request.get(url, headers=api_headers(token))
        if resp.status != 200:
            logger.warning("GET product %s falhou: status %d", product_id, resp.status)
            return None
        payload = resp.json()
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload.get("data")
    except Exception as exc:
        logger.warning("Erro GET product %s: %s", product_id, exc)
    return None


def put_product(page: Page, product_id: str, payload: dict, token: str) -> Tuple[bool, int, str]:
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}"
    try:
        resp = page.request.put(
            url=url,
            data=json.dumps({"data": payload}),
            headers=api_headers(token),
        )
        body = ""
        try:
            body = resp.text()[:500]
        except Exception:
            pass
        return resp.ok, resp.status, body
    except Exception as exc:
        return False, 0, str(exc)


def fetch_all_additional_infos(page: Page, token: str, logger) -> Dict[str, str]:
    info_map: Dict[str, str] = {}
    page_num = 1

    while True:
        url = f"{DESTINO_BASE}/admin/api/additional-info?sort=id&page[size]=25&page[number]={page_num}"
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                logger.warning("Falha GET additional-info pág %d: status %d", page_num, resp.status)
                break
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                name = (item.get("custom_name") or item.get("name") or "").strip()
                info_id = item.get("id")
                if name and info_id:
                    info_map[normalize(name)] = str(info_id)

            total = data.get("paging", {}).get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.6))
        except Exception as exc:
            logger.warning("Erro additional-info pág %d: %s", page_num, exc)
            break

    logger.info("📋 Catálogo de infos adicionais DESTINO: %d entradas", len(info_map))
    return info_map


def fetch_all_additional_infos_catalog(page: Page, token: str, logger) -> Dict[str, dict]:
    catalog: Dict[str, dict] = {}
    page_num = 1

    while True:
        url = f"{DESTINO_BASE}/admin/api/additional-info?sort=id&page[size]=25&page[number]={page_num}"
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                logger.warning("Falha GET additional-info catálogo pág %d: status %d", page_num, resp.status)
                break

            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                name = (item.get("custom_name") or item.get("name") or "").strip()
                item_id = item.get("id")
                if not name or not item_id:
                    continue

                option_map: Dict[str, str] = {}
                options = item.get("options")

                if isinstance(options, list):
                    for option in options:
                        if not isinstance(option, dict):
                            continue
                        option_name = (option.get("name") or "").strip()
                        option_id = option.get("id")
                        if option_name and option_id:
                            option_map[normalize(option_name)] = str(option_id)
                elif isinstance(options, dict):
                    for option in options.values():
                        if not isinstance(option, dict):
                            continue
                        option_name = (option.get("name") or "").strip()
                        option_id = option.get("id")
                        if option_name and option_id:
                            option_map[normalize(option_name)] = str(option_id)

                catalog[normalize(name)] = {
                    "id": str(item_id),
                    "name": name,
                    "option_map": option_map,
                }

            total = data.get("paging", {}).get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.6))
        except Exception as exc:
            logger.warning("Erro additional-info catálogo pág %d: %s", page_num, exc)
            break

    logger.info("📋 Catálogo rico de infos adicionais DESTINO: %d entradas", len(catalog))
    return catalog


def _post_form_urlencoded(page: Page, url: str, form_data: Dict[str, str], referer: str = "") -> Tuple[int, str]:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        headers["Referer"] = referer

    body = urlencode({k: "" if v is None else str(v) for k, v in form_data.items()})
    try:
        resp = page.request.post(url, data=body, headers=headers)
        text = ""
        try:
            text = resp.text()[:300]
        except Exception:
            pass
        return resp.status, text
    except Exception as exc:
        return 0, str(exc)


def _create_additional_info_field(page: Page, info_name: str, logger) -> bool:
    form_data = {
        "nome_loja": info_name,
        "nome_adm": info_name,
        "ativa": "1",
        "exibir_valor": "0",
        "obrigatorio": "1",
        "contador": "0",
        "tipo": "S",
        "valor": "0.00",
        "add_total": "1",
        "ordem": "0",
    }

    endpoints = [
        f"{DESTINO_BASE}/adm/extras/informacao_produto_executar.php?acao=incluir",
        f"{DESTINO_BASE}/admin/informacao_produto_executar.php?acao=incluir",
    ]
    referer = f"{DESTINO_BASE}/admin/#/adm/extras/informacao_produto_index.php"

    for endpoint in endpoints:
        status, body = _post_form_urlencoded(page, endpoint, form_data, referer=referer)
        if status in (200, 201, 302):
            logger.info("🆕 Campo Additional Info criado via %s (status %s): %s", endpoint, status, info_name)
            return True
        logger.warning("Falha criando campo '%s' em %s: status=%s body=%s", info_name, endpoint, status, body)

    return False


def _create_additional_info_option(page: Page, field_id: str, option_name: str, logger) -> bool:
    endpoint = (
        f"{DESTINO_BASE}/adm/extras/informacao_produto_index.php"
        f"?id={field_id}&aba=opcoes&acao=adicionar"
    )
    form_data = {
        "id": str(field_id),
        "id_opcao": "0",
        "exibicao_novo": "1",
        "opcao": option_name,
        "valor": "0.00",
        "imagem": "",
    }
    status, body = _post_form_urlencoded(page, endpoint, form_data)
    if status in (200, 201, 302):
        logger.info("    🆕 Opção criada para field_id=%s: %s", field_id, option_name)
        return True

    logger.warning(
        "    Falha criando opção '%s' em field_id=%s: status=%s body=%s",
        option_name,
        field_id,
        status,
        body,
    )
    return False


def ensure_additional_info_with_options(
    page: Page,
    token: str,
    info_name: str,
    option_names: List[str],
    logger,
) -> Optional[dict]:
    catalog = fetch_all_additional_infos_catalog(page, token, logger=logger)
    normalized_name = normalize(info_name)
    info = catalog.get(normalized_name)

    if not info:
        if not _create_additional_info_field(page, info_name, logger=logger):
            return None
        time.sleep(random.uniform(0.4, 0.9))
        catalog = fetch_all_additional_infos_catalog(page, token, logger=logger)
        info = catalog.get(normalized_name)
        if not info:
            logger.warning("Campo '%s' foi criado, mas não apareceu no catálogo após refresh", info_name)
            return None

    info_id = str(info.get("id"))
    option_map = info.get("option_map") if isinstance(info.get("option_map"), dict) else {}

    for option_name in option_names:
        opt_name = (option_name or "").strip()
        if not opt_name:
            continue
        if normalize(opt_name) in option_map:
            continue
        _create_additional_info_option(page, info_id, opt_name, logger=logger)

    time.sleep(random.uniform(0.3, 0.8))
    refreshed = fetch_all_additional_infos_catalog(page, token, logger=logger).get(normalized_name)
    return refreshed if isinstance(refreshed, dict) else info


def get_product_current_infos(page: Page, product_id: str, logger) -> List[str]:
    url = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    try:
        resp = page.request.get(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        if resp.status != 200:
            logger.warning("GET infos do produto %s: status %d", product_id, resp.status)
            return []

        raw_html = resp.body()
        try:
            html = raw_html.decode("utf-8")
        except UnicodeDecodeError:
            html = raw_html.decode("latin-1", errors="ignore")
        current_ids = []

        pattern_selected = re.findall(
            r'name=["\']selected_items\[\]["\'].*?value=["\'](\d+)["\']',
            html,
            re.DOTALL,
        )
        if pattern_selected:
            current_ids.extend(pattern_selected)

        pattern_checked = re.findall(
            r'<input[^>]*checked[^>]*value=["\'](\d+)["\'][^>]*name=["\'].*?selected',
            html,
            re.DOTALL,
        )
        if pattern_checked:
            current_ids.extend(pattern_checked)

        pattern_checked2 = re.findall(
            r'value=["\'](\d+)["\'][^>]*checked',
            html,
            re.DOTALL,
        )
        if pattern_checked2:
            current_ids.extend(pattern_checked2)

        pattern_sort = re.findall(r'sort\[\].*?value=["\'](\d+)', html, re.DOTALL)
        if pattern_sort:
            current_ids.extend(pattern_sort)

        current_ids = list(set(current_ids))
        logger.info(
            "📎 Produto %s: %d infos adicionais atualmente vinculadas: %s",
            product_id,
            len(current_ids),
            current_ids,
        )
        return current_ids

    except Exception as exc:
        logger.warning("Erro ao buscar infos atuais do produto %s: %s", product_id, exc)
        return []


def post_additional_infos(
    page: Page,
    product_id: str,
    info_ids_to_link: List[str],
    short_delay,
    sort_entries: Optional[List[str]] = None,
    option_info_entries: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    nav_url = (
        f"{DESTINO_BASE}/admin/#/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    try:
        page.goto(nav_url, wait_until="networkidle", timeout=15000)
        short_delay()
    except Exception:
        pass

    parts = ["_method=POST", "_method=POST"]
    for info_id in info_ids_to_link:
        parts.append(f"selected_items%5B%5D={info_id}")
    parts.append(f"id_produto={product_id}")
    parts.append("data%5BAdditionalProductInfo%5D%5Bherda_prazo%5D=0")
    parts.append("data%5BAdditionalProductInfo%5D%5Bprazo%5D=0")

    if sort_entries:
        for entry in sort_entries:
            parts.append(f"sort%5B%5D={entry}")
    else:
        for info_id in info_ids_to_link:
            parts.append(f"sort%5B%5D={info_id}-")

    if option_info_entries:
        for entry in option_info_entries:
            parts.append(f"option_info%5B%5D={entry}")

    body = "&".join(parts)

    endpoint = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )

    try:
        result = page.evaluate(
            """
            async ([url, body]) => {
                try {
                    const resp = await fetch(url, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                        body: body,
                        redirect: 'follow',
                        credentials: 'include',
                    });
                    return {
                        status: resp.status, ok: resp.ok,
                        redirected: resp.redirected,
                        snippet: (await resp.text()).substring(0, 300),
                    };
                } catch(e) { return {status: 0, error: e.message}; }
            }
            """,
            [endpoint, body],
        )

        ok = result.get("ok") or result.get("status") in (200, 302)
        detail = f"status={result.get('status')}, redirected={result.get('redirected')}"
        return ok, detail
    except Exception as exc:
        return False, str(exc)


def get_destino_variants(page: Page, product_id: str, token: str, logger) -> Tuple[list, dict]:
    all_variants = []
    page_num = 1

    while True:
        url = (
            f"{DESTINO_BASE}/admin/api/products/{product_id}/variants"
            f"?sort=order&page[size]=25&page[number]={page_num}"
        )
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                logger.warning("GET variants %s pág %d: status %d", product_id, page_num, resp.status)
                break
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            all_variants.extend(items)

            paging = data.get("paging", {})
            total = paging.get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.5))
        except Exception as exc:
            logger.warning("Erro GET variants pág %d: %s", page_num, exc)
            break

    logger.info("📦 Produto %s: %d variações no DESTINO", product_id, len(all_variants))
    return all_variants, {}


def get_destino_properties(page: Page, token: str, logger) -> Dict[str, str]:
    prop_map: Dict[str, str] = {}
    page_num = 1

    while True:
        url = f"{DESTINO_BASE}/admin/api/properties?sort=id&page[size]=25&page[number]={page_num}"
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                break
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                name = (item.get("name") or "").strip()
                prop_id = item.get("id")
                if name and prop_id:
                    prop_map[normalize(name)] = str(prop_id)

            total = data.get("paging", {}).get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.5))
        except Exception as exc:
            logger.warning("Erro GET properties pág %d: %s", page_num, exc)
            break

    logger.info("🏷️ Propriedades DESTINO: %d", len(prop_map))
    for key, value in prop_map.items():
        logger.info("    '%s' → ID %s", key, value)
    return prop_map


def get_property_values(page: Page, property_id: str, token: str, logger) -> Dict[str, str]:
    url = f"{DESTINO_BASE}/admin/api/properties/{property_id}"
    try:
        resp = page.request.get(url, headers=api_headers(token))
        if resp.status != 200:
            return {}
        data = resp.json()
        prop_data = data.get("data", {})
        values: Dict[str, str] = {}
        for property_value in (prop_data.get("PropertyValues") or []):
            name = (property_value.get("name") or "").strip()
            value_id = property_value.get("id")
            if name and value_id:
                values[normalize(name)] = str(value_id)
        return values
    except Exception as exc:
        logger.warning("Erro GET property %s values: %s", property_id, exc)
        return {}


def append_property_value(page: Page, property_id: str, value_name: str, token: str, logger) -> Optional[str]:
    url = f"{DESTINO_BASE}/admin/api/properties/{property_id}/append-values"
    payload = {"data": {"name": value_name}}

    try:
        resp = page.request.post(
            url=url,
            data=json.dumps(payload),
            headers=api_headers(token),
        )
        if resp.status == 200:
            data = resp.json()
            prop_values = data.get("data", {}).get("PropertyValues", [])
            for property_value in reversed(prop_values):
                if normalize(property_value.get("name", "")) == normalize(value_name):
                    value_id = str(property_value["id"])
                    logger.info("    ✅ Valor '%s' criado → ID %s", value_name, value_id)
                    return value_id
            if prop_values:
                value_id = str(prop_values[-1]["id"])
                logger.info("    ✅ Valor '%s' criado (último) → ID %s", value_name, value_id)
                return value_id
        else:
            body = ""
            try:
                body = resp.text()[:300]
            except Exception:
                pass
            logger.error("    ❌ append-values falhou: status %d — %s", resp.status, body)
    except Exception as exc:
        logger.error("    ❌ append-values erro: %s", exc)

    return None


def delete_variant(page: Page, variant_id: str, token: str, logger) -> bool:
    url = f"{DESTINO_BASE}/admin/api/products-variants/{variant_id}"
    try:
        resp = page.request.delete(url, headers=api_headers(token))
        if resp.status == 204:
            logger.info("    🗑️ Variação %s deletada", variant_id)
            return True
        logger.warning("    ❌ DELETE variação %s: status %d", variant_id, resp.status)
        return False
    except Exception as exc:
        logger.error("    ❌ DELETE variação %s erro: %s", variant_id, exc)
        return False


def put_variants(page: Page, product_id: str, variants_payload: list, token: str) -> Tuple[bool, int, str]:
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}/variants"
    try:
        resp = page.request.put(
            url=url,
            data=json.dumps({"data": variants_payload}),
            headers=api_headers(token),
        )
        body = ""
        try:
            body = resp.text()[:500]
        except Exception:
            pass
        return resp.ok, resp.status, body
    except Exception as exc:
        return False, 0, str(exc)


def post_variants(page: Page, product_id: str, variants_payload: list, token: str) -> Tuple[bool, int, str]:
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}/variants"
    try:
        resp = page.request.post(
            url=url,
            data=json.dumps({"data": variants_payload}),
            headers=api_headers(token),
        )
        body = ""
        try:
            body = resp.text()[:500]
        except Exception:
            pass
        return resp.ok, resp.status, body
    except Exception as exc:
        return False, 0, str(exc)
