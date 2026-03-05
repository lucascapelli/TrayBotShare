# ========================== destino_api.py (PATCHES INTEGRADOS) ==========================
#
# CHANGELOG (patches sobre versão anterior):
#   [PATCH-A] fetch_origin_variants_full() — NOVO (melhorado)
#             Busca os objetos COMPLETAS de variação via GET /variants na origem.
#             Aceita autenticação via Cookie e/ou Authorization header.
#
#   [PATCH-B] fetch_origin_auth_token() — NOVO
#             Intercepta o Bearer token que a página de edição da origem usa,
#             abrindo a página e monitorando as requisições XHR.
#
#   [PATCH-C] _parse_origin_html_options() — MELHORADO
#             (mantido)
#
# Ajustes adicionais nesta versão:
#   - fetch_origin_variants_full aceita token fallback quando cookies ausentes
#   - read_origin_checked_options tenta múltiplas URLs e usa token quando cookies falham
#   - adicionadas funções utilitárias para extrair PropertyValue IDs de variantes
#   - logs adicionais para esclarecer counts e método de autenticação
#
# Todos os demais métodos mantidos ou levemente adaptados para compatibilidade.

import json
import logging
import random
import re
import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

from patchright.sync_api import Page

from .config import DESTINO_BASE
from .domain import api_headers, normalize

_logger = logging.getLogger("sync")


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _build_cookie_header(cookies_origem) -> str:
    """
    Constrói a string do header Cookie a partir de cookies_origem,
    aceitando AMBOS os formatos:
      - Lista de dicts Playwright: [{"name": "x", "value": "y"}, ...]
      - Lista de strings: ["name=value", ...]
      - String única: "name=value; name2=value2"
    """
    if not cookies_origem:
        return ""
    if isinstance(cookies_origem, str):
        return cookies_origem

    parts = []
    for c in cookies_origem:
        if isinstance(c, dict):
            name  = c.get("name") or ""
            value = c.get("value") or ""
            if name:
                parts.append(f"{name}={value}")
        elif isinstance(c, str):
            parts.append(c)

    return "; ".join(parts)


def extract_property_value_ids_from_variant(variant: dict) -> Set[str]:
    """
    Extrai possíveis PropertyValue IDs (ou strings numéricas representando ids)
    do objeto variant retornado pelo endpoint /variants. Trabalha com vários
    formatos possíveis que as variações podem apresentar (chaves comuns,
    lista de Sku/PropertyValue, etc).
    """
    pv_ids: Set[str] = set()

    if not isinstance(variant, dict):
        return pv_ids

    # Checar campos óbvios
    candidates = [
        "PropertyValue", "PropertyValues", "PropertyValueIds", "PropertyValuesList",
        "Property_value", "property_values", "PropertyValueId", "propertyValue"
    ]
    for key in candidates:
        items = variant.get(key)
        if not items:
            continue
        # normalizar dict/list
        if isinstance(items, dict):
            items_iter = items.values()
        elif isinstance(items, list):
            items_iter = items
        else:
            items_iter = [items]
        for it in items_iter:
            if isinstance(it, dict):
                # procurar id em várias chaves
                for candidate_key in ("id", "value_id", "property_value_id", "PropertyValueId", "pv_id"):
                    val = it.get(candidate_key)
                    if val:
                        pv_ids.add(str(val))
                        break
                # também pode conter name+id
                if "id" in it and str(it.get("id")).isdigit():
                    pv_ids.add(str(it.get("id")))
            elif isinstance(it, str) and it.isdigit():
                pv_ids.add(it)

    # Sku pode ter formato [{"type":"Aro","value":"12"}, ...] ou strings que referenciam pv ids
    skus = variant.get("Sku") or variant.get("Skus") or variant.get("sku") or variant.get("skus")
    if skus:
        if isinstance(skus, list):
            for s in skus:
                if isinstance(s, dict):
                    # tente extrair id-like fields
                    for candidate in ("id", "value_id", "pv_id"):
                        v = s.get(candidate)
                        if v and str(v).isdigit():
                            pv_ids.add(str(v))
                    # talvez value seja numérico (ex: "12") — mas não é pv id; ignorar
                elif isinstance(s, str) and s.isdigit():
                    pv_ids.add(s)

    # Em alguns formatos, variant pode incluir 'values' ou 'property_values' simples
    for key in ("values", "values_list", "property_values"):
        items = variant.get(key)
        if not items:
            continue
        if isinstance(items, dict):
            items_iter = items.values()
        elif isinstance(items, list):
            items_iter = items
        else:
            items_iter = [items]
        for it in items_iter:
            if isinstance(it, dict) and it.get("id"):
                pv_ids.add(str(it.get("id")))
            elif isinstance(it, str) and it.isdigit():
                pv_ids.add(it)

    # Último recurso: olhar campos que pareçam "property_value_ids" como string "1,2,3"
    for key in variant.keys():
        if "property" in key.lower() and "id" in key.lower():
            val = variant.get(key)
            if isinstance(val, str):
                for p in re.split(r"[,\s;]+", val):
                    if p.isdigit():
                        pv_ids.add(p)
            elif isinstance(val, list):
                for p in val:
                    if isinstance(p, (int, str)) and str(p).isdigit():
                        pv_ids.add(str(p))

    return pv_ids


def collect_property_value_ids_from_variants(variants: List[dict]) -> List[str]:
    all_ids: Set[str] = set()
    for v in variants:
        all_ids.update(extract_property_value_ids_from_variant(v))
    return sorted(all_ids, key=lambda x: int(x) if x.isdigit() else x)


# ══════════════════════════════════════════════════════════════════════════════
# [PATCH-A] fetch_origin_variants_full — NOVO (aceita token fallback)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_origin_variants_full(
    page: Page,
    origin_base: str,
    product_id: str,
    cookies_origem,
    logger,
    token: str = "",
) -> list:
    """
    [PATCH-A] Busca as variações COMPLETAS da origem via API REST.

    AUTENTICAÇÃO:
      Tenta Cookie primeiro (sessão do browser), depois tenta Authorization
      se cookie ausente ou retornar 401/403. Aceita ambos os formatos de cookies_origem.
    """
    cookie_str = _build_cookie_header(cookies_origem)
    all_variants = []
    page_num = 1
    used_auth_method = "none"

    while True:
        url = (
            f"{origin_base}/admin/api/products/{product_id}/variants"
            f"?sort=order&page[size]=50&page[number]={page_num}"
        )

        headers = {
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{origin_base}/admin/products/product/{product_id}",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        }

        if cookie_str:
            headers["Cookie"] = cookie_str
            used_auth_method = "cookie"
        elif token:
            headers["Authorization"] = token
            used_auth_method = "token"

        try:
            resp = page.request.get(url, headers=headers, timeout=20_000)

            if resp.status in (401, 403) and used_auth_method == "cookie" and token:
                # tentar com token se cookie negado
                headers.pop("Cookie", None)
                headers["Authorization"] = token
                used_auth_method = "token"
                resp = page.request.get(url, headers=headers, timeout=20_000)

            if resp.status == 401:
                logger.warning(
                    "⚠️ [PATCH-A] 401 Unauthorized — autenticação falhou (produto %s)", product_id
                )
                break

            if resp.status == 403:
                logger.warning(
                    "⚠️ [PATCH-A] 403 Forbidden — sem permissão para variants (produto %s)", product_id
                )
                break

            if resp.status != 200:
                logger.warning(
                    "⚠️ [PATCH-A] fetch_origin_variants_full produto %s pág %d: status %d",
                    product_id, page_num, resp.status,
                )
                break

            # Verificar se caiu na página de login (redirect HTML)
            content_type = resp.headers.get("content-type") or ""
            if "application/json" not in content_type:
                logger.warning(
                    "⚠️ [PATCH-A] Resposta não-JSON (content-type: %s) — possível redirect para login (produto %s)",
                    content_type, product_id,
                )
                break

            data = resp.json()
            items = data.get("data") or []
            if not items:
                break

            all_variants.extend(items)

            # Paginação
            paging = data.get("paging") or {}
            total = int(paging.get("total") or 0)
            total_pages = max(1, (total + 49) // 50)

            if page_num >= total_pages:
                break

            page_num += 1
            time.sleep(random.uniform(0.25, 0.6))

        except Exception as exc:
            logger.warning(
                "⚠️ [PATCH-A] Erro fetch_origin_variants_full produto %s pág %d: %s",
                product_id, page_num, exc,
            )
            break

    if all_variants:
        # Log resumido: quantas têm SKU / PropertyValue data útil
        try:
            from .domain import _extract_sku_items_from_variant
        except Exception:
            _extract_sku_items_from_variant = lambda v: []
        with_sku = sum(1 for v in all_variants if _extract_sku_items_from_variant(v))
        # contar quantas têm PropertyValue ids detectáveis
        with_pv = sum(1 for v in all_variants if extract_property_value_ids_from_variant(v))
        logger.info(
            "📦 [PATCH-A] Origem produto %s: %d variações carregadas (%d com SKU, %d com PV ids, %d só IDs)",
            product_id,
            len(all_variants),
            with_sku,
            with_pv,
            len(all_variants) - with_pv,
        )
    else:
        logger.warning("⚠️ [PATCH-A] Nenhuma variação retornada para produto %s", product_id)

    logger.info("🔐 [PATCH-A] método autenticação usado para variants: %s", used_auth_method)
    return all_variants


def fetch_origin_variant_details(
    page: Page,
    origin_base: str,
    product_id: str,
    variant_id: str,
    cookies_origem,
    logger,
    token: str = "",
) -> dict:
    """
    Busca detalhes completos de UMA variação da origem (com type/value).
    Tenta endpoint provável: /admin/api/products/{product_id}/variants/{variant_id}
    Se retornar 404, tenta /admin/api/variants/{variant_id} como fallback.
    Retorna o objeto variant (ou {} em erro).
    """
    cookie_str = _build_cookie_header(cookies_origem)
    headers = {
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{origin_base}/admin/products/{product_id}/edit",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    if cookie_str:
        headers["Cookie"] = cookie_str
    if token:
        headers["Authorization"] = token

    urls = [
        f"{origin_base}/admin/api/products/{product_id}/variants/{variant_id}",
        f"{origin_base}/admin/api/variants/{variant_id}",
    ]

    for url in urls:
        try:
            resp = page.request.get(url, headers=headers, timeout=15_000)
            if resp.status == 404:
                logger.debug("GET variant %s returned 404 for url %s", variant_id, url)
                continue
            if resp.status != 200:
                logger.warning("GET variant %s falhou (url=%s): %d", variant_id, url, resp.status)
                continue

            content_type = resp.headers.get("content-type") or ""
            if "application/json" not in content_type:
                logger.warning("GET variant %s resposta não-JSON (url=%s): %s", variant_id, url, content_type)
                return {}

            data = resp.json()
            variant_data = data.get("data") or data

            # Tentar extrair sku/property values
            sku = variant_data.get("Sku") or variant_data.get("sku") or []
            if sku:
                logger.info("   → Variant %s: sku encontrado com %d itens", variant_id, len(sku))
                return {"sku": sku}

            pv = variant_data.get("PropertyValue") or variant_data.get("PropertyValues") or []
            if pv:
                logger.info("   → Variant %s: PropertyValues encontrado com %d itens", variant_id, len(pv))
                return {"PropertyValue": pv}

            # Se não encontrarmos os formatos esperados, retornar raw
            logger.warning("   → Variant %s: sem sku nem PropertyValue (url=%s)", variant_id, url)
            return variant_data

        except Exception as e:
            logger.error("Erro fetch variant %s (url=%s): %s", variant_id, url, e)
            continue

    return {}


# ══════════════════════════════════════════════════════════════════════════════
# [PATCH-B] fetch_origin_auth_token — NOVO (melhorado: tenta múltiplas URLs)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_origin_auth_token(
    page: Page,
    origin_base: str,
    product_id: str,
    cookies_origem,
    logger,
) -> str:
    """
    [PATCH-B] Intercepta o Bearer token que a página de edição da ORIGEM usa.
    Tenta navegar para vários caminhos comuns da área administrativa.
    """
    found_token: list = []

    def _on_response(response):
        if found_token:
            return
        try:
            url = response.url
            ct = (response.headers.get("content-type") or "")
            if f"/admin/api/products/{product_id}" in url and "application/json" in ct:
                req_auth = response.request.headers.get("authorization") or ""
                if req_auth and len(req_auth) > 10:
                    found_token.append(req_auth)
        except Exception:
            pass

    page.on("response", _on_response)

    try:
        candidates = [
            f"{origin_base}/admin/products/product/{product_id}",
            f"{origin_base}/admin/products/{product_id}/edit",
            f"{origin_base}/admin/products/{product_id}",
        ]
        for url in candidates:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=12_000)
            except Exception:
                try:
                    page.goto(url, wait_until="networkidle", timeout=18_000)
                except Exception:
                    pass
            # Aguarda curto período por interceptação
            waited = 0
            while not found_token and waited < 2_000:
                page.wait_for_timeout(250)
                waited += 250
            if found_token:
                break

    except Exception as exc:
        logger.warning(
            "⚠️ [PATCH-B] fetch_origin_auth_token erro ao navegar (produto %s): %s",
            product_id, exc,
        )
    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            try:
                page.off("response", _on_response)
            except Exception:
                pass

    token = found_token[0] if found_token else ""
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"

    if token:
        logger.info("🔑 [PATCH-B] Token da origem capturado (produto %s): %s...", product_id, token[:35])
    else:
        logger.warning("⚠️ [PATCH-B] Token da origem NÃO encontrado (produto %s)", product_id)

    return token


# ══════════════════════════════════════════════════════════════════════════════
# Product CRUD (sem alterações)
# ══════════════════════════════════════════════════════════════════════════════

def get_product_details(
    page: Page, product_id: str, token: str, logger
) -> Optional[dict]:
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}"
    try:
        resp = page.request.get(url, headers=api_headers(token))
        if resp.status != 200:
            logger.warning(
                "GET product %s falhou: status %d", product_id, resp.status
            )
            return None
        payload = resp.json()
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload.get("data")
    except Exception as exc:
        logger.warning("Erro GET product %s: %s", product_id, exc)
    return None


def put_product(
    page: Page, product_id: str, payload: dict, token: str
) -> Tuple[bool, int, str]:
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


# ══════════════════════════════════════════════════════════════════════════════
# Additional Info Catalog (sem alterações)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_additional_infos(
    page: Page, token: str, logger
) -> Dict[str, str]:
    info_map: Dict[str, str] = {}
    page_num = 1

    while True:
        url = (
            f"{DESTINO_BASE}/admin/api/additional-info"
            f"?sort=id&page[size]=25&page[number]={page_num}"
        )
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                logger.warning(
                    "Falha GET additional-info pág %d: status %d",
                    page_num, resp.status,
                )
                break
            data  = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                name    = (item.get("custom_name") or item.get("name") or "").strip()
                info_id = item.get("id")
                if name and info_id:
                    info_map[normalize(name)] = str(info_id)

            total       = data.get("paging", {}).get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.6))
        except Exception as exc:
            logger.warning("Erro additional-info pág %d: %s", page_num, exc)
            break

    logger.info(
        "📋 Catálogo de infos adicionais DESTINO: %d entradas", len(info_map)
    )
    return info_map


def fetch_all_additional_infos_catalog(
    page: Page, token: str, logger
) -> Dict[str, dict]:
    catalog: Dict[str, dict] = {}
    page_num = 1

    while True:
        url = (
            f"{DESTINO_BASE}/admin/api/additional-info"
            f"?sort=id&page[size]=25&page[number]={page_num}"
        )
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                logger.warning(
                    "Falha GET additional-info catálogo pág %d: status %d",
                    page_num, resp.status,
                )
                break

            data  = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                name    = (item.get("custom_name") or item.get("name") or "").strip()
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
                        option_id   = option.get("id")
                        if option_name and option_id:
                            option_map[normalize(option_name)] = str(option_id)
                elif isinstance(options, dict):
                    for option in options.values():
                        if not isinstance(option, dict):
                            continue
                        option_name = (option.get("name") or "").strip()
                        option_id   = option.get("id")
                        if option_name and option_id:
                            option_map[normalize(option_name)] = str(option_id)

                catalog[normalize(name)] = {
                    "id":         str(item_id),
                    "name":       name,
                    "option_map": option_map,
                }

            total       = data.get("paging", {}).get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.6))
        except Exception as exc:
            logger.warning(
                "Erro additional-info catálogo pág %d: %s", page_num, exc
            )
            break

    logger.info(
        "📋 Catálogo rico de infos adicionais DESTINO: %d entradas", len(catalog)
    )
    return catalog


# ====================== ORIGEM (Tray) helpers ======================
def _extract_origin_token(page: Page) -> str:
    try:
        token = page.evaluate(
            """() => {
                const keys = ['token','access_token','auth_token','authorization','jwt','bearer','api_token'];
                for (const k of keys) {
                    const v = localStorage.getItem(k);
                    if (v && v.length > 10) return v;
                }
                for (let i = 0; i < localStorage.length; i++) {
                    const k = localStorage.key(i);
                    const v = localStorage.getItem(k);
                    if (v && typeof v === 'string' && v.startsWith('eyJ')) return v;
                }
                return null;
            }"""
        )
    except Exception:
        token = ""
    token = (token or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return token


def get_origin_product_details(page: Page, origin_base: str, product_id: str, cookies_origem, logger) -> Optional[dict]:
    """Tenta obter o JSON do produto na ORIGEM via API (usa token extraído do localStorage)."""
    try:
        try:
            page.goto(f"{origin_base}/admin/products/{product_id}/edit", wait_until="domcontentloaded", timeout=12000)
        except Exception:
            try:
                page.goto(f"{origin_base}/admin/products/{product_id}/edit", wait_until="networkidle", timeout=20000)
            except Exception:
                pass

        token = _extract_origin_token(page)
        headers = {
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
        if token:
            headers["Authorization"] = token

        url = f"{origin_base}/admin/api/products/{product_id}"
        resp = page.request.get(url, headers=headers)
        if resp.status != 200:
            logger.warning("GET ORIGEM product %s falhou: status %d", product_id, resp.status)
            return None
        payload = resp.json()
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload.get("data")
    except Exception as exc:
        logger.warning("Erro GET ORIGEM product %s: %s", product_id, exc)
    return None


def put_origin_additional_infos(page: Page, origin_base: str, product_id: str, additional_infos: list, cookies_origem, logger) -> Tuple[bool, int, str]:
    """Atualiza o produto na ORIGEM definindo o campo AdditionalInfos (PUT /admin/api/products/{id})."""
    try:
        try:
            page.goto(f"{origin_base}/admin/products/{product_id}/edit", wait_until="domcontentloaded", timeout=12000)
        except Exception:
            try:
                page.goto(f"{origin_base}/admin/products/{product_id}/edit", wait_until="networkidle", timeout=20000)
            except Exception:
                pass

        token = _extract_origin_token(page)
        headers = {"Accept": "application/json", "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
        if token:
            headers["Authorization"] = token

        url = f"{origin_base}/admin/api/products/{product_id}"
        payload = {"data": {"AdditionalInfos": additional_infos}}
        resp = page.request.put(url=url, data=json.dumps(payload), headers=headers)
        body = ""
        try:
            body = resp.text()[:500]
        except Exception:
            pass
        return resp.ok, resp.status, body
    except Exception as exc:
        return False, 0, str(exc)


def map_origin_variant_ids_to_properties(
    page: Page,
    origin_base: str,
    variant_ids: list,
    cookies_origem,
    logger,
) -> Dict[str, list]:
    """Dado um conjunto de Variant/PropertyValue ids da ORIGEM, tenta mapear cada id
    para a propriedade e nome do valor correspondente consultando
    /admin/api/properties e /admin/api/properties/{id}.

    Nota: variant_ids aqui pode ser uma lista de PropertyValue IDs (recomendado).
    Retorna: { normalized_property_name: [value_name, ...], ... }
    """
    mapping: Dict[str, list] = {}
    if not variant_ids:
        return mapping

    try:
        # navegar para extrair token (opcional)
        try:
            page.goto(f"{origin_base}/admin/products/1/edit", wait_until="domcontentloaded", timeout=8000)
        except Exception:
            pass

        token = _extract_origin_token(page)
        headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
        if token:
            headers["Authorization"] = token

        # obter propriedades (page[size] grande para trazer todas)
        props_url = f"{origin_base}/admin/api/properties?sort=name&page[size]=9999"
        try:
            resp = page.request.get(props_url, headers=headers, timeout=45000)
            if resp.status != 200:
                logger.warning("Falha GET origin properties: status %d", resp.status)
                return mapping
            data = resp.json()
            properties = data.get("data", [])
        except Exception as exc:
            logger.warning("Erro GET origin properties: %s", exc)
            return mapping

        # Preparar set para buscas rápidas
        id_set = {str(i) for i in variant_ids}

        # Para cada property, obter seus valores e verificar ids
        for prop in properties:
            prop_id = str(prop.get("id") or "")
            prop_name = prop.get("name") or ""
            if not prop_id:
                continue
            try:
                purl = f"{origin_base}/admin/api/properties/{prop_id}"
                presp = page.request.get(purl, headers=headers, timeout=30000)
                if presp.status != 200:
                    continue
                pbody = presp.json()
                pv_list = (pbody.get("data") or {}).get("PropertyValues") or []
                for pv in pv_list:
                    vid = str(pv.get("id") or "")
                    vname = pv.get("name") or ""
                    if vid and vid in id_set:
                        norm = normalize(prop_name)
                        mapping.setdefault(norm, [])
                        if vname and vname not in mapping[norm]:
                            mapping[norm].append(vname)
            except Exception:
                continue

        logger.info("🔎 Mapeamento VariantIDs→properties: %d campos encontrados", len(mapping))
        return mapping
    except Exception as exc:
        logger.warning("Erro mapeando variant ids na origem: %s", exc)
        return mapping


def _post_form_urlencoded(
    page: Page,
    url: str,
    form_data: Dict[str, str],
    referer: str = "",
) -> Tuple[int, str]:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        headers["Referer"] = referer

    body = urlencode(
        {k: "" if v is None else str(v) for k, v in form_data.items()}
    )
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


def _create_additional_info_field(
    page: Page, info_name: str, logger
) -> bool:
    form_data = {
        "nome_loja": info_name,
        "nome_adm":  info_name,
        "ativa":     "1",
        "exibir_valor": "0",
        "obrigatorio":  "1",
        "contador":     "0",
        "tipo":         "S",
        "valor":        "0.00",
        "add_total":    "1",
        "ordem":        "0",
    }

    endpoints = [
        f"{DESTINO_BASE}/adm/extras/informacao_produto_executar.php?acao=incluir",
        f"{DESTINO_BASE}/admin/informacao_produto_executar.php?acao=incluir",
    ]
    referer = f"{DESTINO_BASE}/admin/#/adm/extras/informacao_produto_index.php"

    for endpoint in endpoints:
        status, body = _post_form_urlencoded(
            page, endpoint, form_data, referer=referer
        )
        if status in (200, 201, 302):
            logger.info(
                "🆕 Campo Additional Info criado via %s (status %s): %s",
                endpoint, status, info_name,
            )
            return True
        logger.warning(
            "Falha criando campo '%s' em %s: status=%s body=%s",
            info_name, endpoint, status, body,
        )

    return False


def _create_additional_info_option(
    page: Page, field_id: str, option_name: str, logger
) -> bool:
    endpoint = (
        f"{DESTINO_BASE}/adm/extras/informacao_produto_index.php"
        f"?id={field_id}&aba=opcoes&acao=adicionar"
    )
    form_data = {
        "id":          str(field_id),
        "id_opcao":    "0",
        "exibicao_novo": "1",
        "opcao":       option_name,
        "valor":       "0.00",
        "imagem":      "",
    }
    status, body = _post_form_urlencoded(page, endpoint, form_data)
    if status in (200, 201, 302):
        logger.info(
            "    🆕 Opção criada para field_id=%s: %s", field_id, option_name
        )
        return True

    logger.warning(
        "    Falha criando opção '%s' em field_id=%s: status=%s body=%s",
        option_name, field_id, status, body,
    )
    return False


def ensure_additional_info_with_options(
    page: Page,
    token: str,
    info_name: str,
    option_names: List[str],
    logger,
) -> Optional[dict]:
    catalog        = fetch_all_additional_infos_catalog(page, token, logger=logger)
    normalized_name = normalize(info_name)
    info           = catalog.get(normalized_name)

    if not info:
        if not _create_additional_info_field(page, info_name, logger=logger):
            return None
        time.sleep(random.uniform(0.4, 0.9))
        catalog = fetch_all_additional_infos_catalog(page, token, logger=logger)
        info    = catalog.get(normalized_name)
        if not info:
            logger.warning(
                "Campo '%s' foi criado, mas não apareceu no catálogo após refresh",
                info_name,
            )
            return None

    info_id    = str(info.get("id"))
    option_map = info.get("option_map") if isinstance(info.get("option_map"), dict) else {}

    for option_name in option_names:
        opt_name = (option_name or "").strip()
        if not opt_name:
            continue
        if normalize(opt_name) in option_map:
            continue
        _create_additional_info_option(page, info_id, opt_name, logger=logger)

    time.sleep(random.uniform(0.3, 0.8))
    refreshed = fetch_all_additional_infos_catalog(
        page, token, logger=logger
    ).get(normalized_name)
    return refreshed if isinstance(refreshed, dict) else info


# ══════════════════════════════════════════════════════════════════════════════
# LEITURA DE OPÇÕES MARCADAS DA ORIGEM VIA HTTP (melhorada: múltiplas URLs + token)
# ══════════════════════════════════════════════════════════════════════════════

def read_origin_checked_options(
    page: Page,
    origin_base: str,
    product_id: str,
    cookies_origem,
    logger,
) -> Dict[str, List[str]]:
    """
    Lê a página de additional_product_info da ORIGEM via HTTP GET
    e retorna quais opções estão REALMENTE marcadas (checked).

    Melhorias:
      - Tenta múltiplas URLs conhecidas antes de fallback
      - Aceita autenticação por Cookie OU Authorization Bearer token (capturado via fetch_origin_auth_token)
    """
    urls_to_try = [
        f"{origin_base}/mvc/adm/additional_product_info/additional_product_info/edit/{product_id}",
        f"{origin_base}/admin/products/{product_id}/edit",
        f"{origin_base}/admin/products/product/{product_id}",
    ]

    cookie_str = _build_cookie_header(cookies_origem)
    token = ""
    headers_base = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "Referer": f"{origin_base}/admin/products/{product_id}/edit",
    }

    # Try sequence: cookie (if present) -> multiple urls -> if fail, try to capture token and retry
    def try_urls_with_headers(headers) -> Optional[str]:
        for url in urls_to_try:
            try:
                resp = page.request.get(url, headers=headers, timeout=15000)
                if resp.status != 200:
                    logger.debug("GET %s returned status %s", url, resp.status)
                    continue
                raw = resp.body()
                # decode
                try:
                    html = raw.decode("utf-8")
                except UnicodeDecodeError:
                    html = raw.decode("latin-1", errors="ignore")
                # quick sanity
                if len(html) < 200:
                    logger.debug("HTML curto (%d) em %s", len(html), url)
                    continue
                html_lower = html.lower()
                if 'type="password"' in html_lower or "name='password'" in html_lower:
                    logger.debug("Redirect to login detected in %s", url)
                    continue
                return html
            except Exception as exc:
                logger.debug("Erro GET %s: %s", url, exc)
                continue
        return None

    # 1) Try with cookie if available
    if cookie_str:
        headers = dict(headers_base)
        headers["Cookie"] = cookie_str
        html = try_urls_with_headers(headers)
        if html:
            logger.info("🔐 read_origin_checked_options: usando Cookie para autenticação")
            return _parse_origin_html_options(html, logger)

    # 2) Try extracting token from page (if cookie absent or failed)
    try:
        token = fetch_origin_auth_token(page, origin_base, product_id, cookies_origem, logger)
    except Exception as exc:
        logger.debug("Erro capturando token via fetch_origin_auth_token: %s", exc)
        token = ""

    if token:
        headers = dict(headers_base)
        headers["Authorization"] = token
        html = try_urls_with_headers(headers)
        if html:
            logger.info("🔐 read_origin_checked_options: usando Authorization token para autenticação")
            return _parse_origin_html_options(html, logger)

    # 3) As última tentativa: usar Playwright rendering (sem adicionar cookies) para tentar coletar via interação
    logger.warning("⚠️ read_origin_checked_options: falha em GET HTML via cookie/token; tentativa com Playwright navegacional")
    try:
        return read_origin_checked_options_playwright(page, origin_base, product_id, cookies_origem, logger)
    except Exception as exc:
        logger.warning("Erro fallback Playwright na leitura de opções ORIGEM: %s", exc)
        return {}


def read_origin_checked_options_playwright(
    page: Page,
    origin_base: str,
    product_id: str,
    cookies_origem,
    logger,
) -> Dict[str, List[str]]:
    """
    (mantida) Coleta opções marcadas/visíveis na página de edição do produto NA ORIGEM
    usando interação Playwright (navegação + cliques). Retorna mapa
    normalized_field_name -> [label1, label2, ...]
    """
    result = {}
    try:
        # Tentar adicionar cookies ao contexto para manter sessão
        try:
            context = getattr(page, "context", None)
            if context and cookies_origem:
                cookies_to_add = []
                # aceitar formatos: list of dicts ou list of strings
                for c in cookies_origem:
                    if isinstance(c, dict):
                        name = c.get("name") or c.get("Name")
                        value = c.get("value") or c.get("Value")
                        if name and value:
                            cookies_to_add.append({"name": name, "value": value, "url": origin_base})
                    elif isinstance(c, str) and "=" in c:
                        parts = c.split("=", 1)
                        cookies_to_add.append({"name": parts[0].strip(), "value": parts[1].strip(), "url": origin_base})
                if cookies_to_add:
                    try:
                        context.add_cookies(cookies_to_add)
                    except Exception:
                        pass
        except Exception:
            pass

        url = f"{origin_base}/admin/products/{product_id}/edit"
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
            except Exception as e:
                logger.warning("⚠️ Navegação ORIGEM falhou: %s", e)
                return {}

        # localizar o container de variações
        try:
            container = page.locator("#product-variations-form")
            if container.count() == 0:
                container = None
        except Exception:
            container = None

        # coletar possíveis labels/fields
        field_texts = []
        try:
            if container is not None:
                try:
                    field_texts = container.locator("label, h3, h4, .variation-label, .field-label, legend, .title, .form-group").all_text_contents()
                except Exception:
                    field_texts = []
            else:
                try:
                    field_texts = page.locator("label, h3, h4, .variation-label, .field-label, legend, .title, .form-group").all_text_contents()
                except Exception:
                    field_texts = []
        except Exception:
            field_texts = []

        # Normalizar e deduplicar
        seen_fields = []
        for t in field_texts:
            s = (t or "").strip()
            if not s:
                continue
            n = normalize(s)
            if n not in seen_fields:
                seen_fields.append(n)

        logger.info("🔎 ORIGEM fields detectados (amostra): %s", seen_fields[:30])

        # Para cada field detectado, tentar clicar e coletar opções visíveis
        for raw in list(dict.fromkeys(field_texts)):
            title = (raw or "").strip()
            if not title:
                continue
            norm_title = normalize(title)
            options = []
            try:
                # tentar clicar no campo
                try:
                    locator = None
                    if container is not None:
                        locator = container.get_by_text(title)
                        if locator.count() == 0:
                            locator = page.get_by_text(title)
                    else:
                        locator = page.get_by_text(title)
                    if locator and locator.count() > 0:
                        try:
                            locator.first.click()
                        except Exception:
                            try:
                                locator.first.scroll_into_view_if_needed()
                                locator.first.click()
                            except Exception:
                                pass
                except Exception:
                    pass

                # breve espera
                try:
                    page.wait_for_timeout(300)
                except Exception:
                    pass

                # coletar opções no container
                opts_nodes = None
                try:
                    if container is not None:
                        opts_nodes = container.locator("[role='option'], .dropdown-menu li, li, button, a, span, .option, .variation-option")
                    else:
                        opts_nodes = page.locator("[role='option'], .dropdown-menu li, li, button, a, span, .option, .variation-option")
                except Exception:
                    opts_nodes = None

                texts = []
                try:
                    if opts_nodes is not None:
                        texts = opts_nodes.all_text_contents()
                except Exception:
                    texts = []

                for tt in texts:
                    v = (tt or "").strip()
                    if not v:
                        continue
                    if normalize(v) == norm_title:
                        continue
                    if v not in options:
                        options.append(v)

                # fallback: coletar textos curtos do container
                if not options and container is not None:
                    try:
                        all_texts = container.all_text_contents()
                        for at in all_texts:
                            s = (at or "").strip()
                            if not s or len(s) > 30:
                                continue
                            if normalize(s) == norm_title:
                                continue
                            if s not in options:
                                options.append(s)
                    except Exception:
                        pass

            except Exception as exc:
                logger.debug("Erro ao coletar opções para field '%s': %s", title, exc)

            if options:
                result[norm_title] = options

        logger.info("🔎 ORIGEM options coletadas: %d campos", len(result))
        return result
    except Exception as exc:
        logger.warning("Erro geral coleta ORIGEM via Playwright: %s", exc)
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# [PATCH-C] _parse_origin_html_options — MELHORADO
# ══════════════════════════════════════════════════════════════════════════════

def _parse_origin_html_options(
    html: str, logger
) -> Dict[str, List[str]]:
    """
    [PATCH-C] Parseia HTML da página additional_product_info da ORIGEM.
    (mantido/sem mudanças relevantes além de logs)
    """
    field_id_to_name: Dict[str, str] = {}

    # ── Padrão 1: <div|section id="field_NNN|info_NNN"> → <label|h*|strong>Nome</tag>
    for m in re.finditer(
        r'id=["\'](?:field_|info_|item_)?(\d{2,6})["\'][^>]*>'
        r'(?:(?!<(?:div|section|form)\b).){0,400}'
        r'<(?:label|h[2-6]|strong|span)[^>]*>\s*([^<]{2,80})\s*</(?:label|h[2-6]|strong|span)>',
        html,
        re.IGNORECASE | re.DOTALL,
    ):
        fid   = m.group(1)
        fname = m.group(2).strip().rstrip(':').strip()
        if fname and fid not in field_id_to_name:
            field_id_to_name[fid] = fname

    # ── Padrão 2: texto "NNN-NomeCampo" — Nome começa com maiúscula/acento
    for m in re.finditer(
        r'\b(\d{2,6})\s*[-–]\s*([A-ZÁÉÍÓÚÀÂÊÎÔÛÃÕÇ][a-zA-ZáéíóúàâêîôûãõçA-Z0-9 ]{2,60})',
        html,
    ):
        fid   = m.group(1)
        fname = m.group(2).strip().rstrip(':').strip()
        if fname and fid not in field_id_to_name:
            field_id_to_name[fid] = fname

    # ── Padrão 3 (fallback original): qualquer "NNN - texto de 2-60 chars"
    for m in re.finditer(r'(\d{2,6})\s*[-–]\s*([^<"\n\r]{2,60})', html):
        fid   = m.group(1)
        fname = m.group(2).strip().rstrip(':').strip()
        if fname and fid not in field_id_to_name:
            field_id_to_name[fid] = fname

    # ── Passo 2: Encontrar todos os checkboxes option_info ──
    checked_by_field:   Dict[str, List[str]] = {}
    unchecked_by_field: Dict[str, int]       = {}

    for match in re.finditer(
        r'<input\b([^>]*)>([^<]{0,120})',
        html,
        re.IGNORECASE | re.DOTALL,
    ):
        attrs      = match.group(1)
        label_text = match.group(2).strip()

        # Só checkboxes de option_info
        if not re.search(r'type\s*=\s*["\']checkbox["\']', attrs, re.IGNORECASE):
            continue
        if "option_info" not in attrs:
            continue

        # Extrair value (formato: OPTION_ID-FIELD_ID)
        value_match = re.search(
            r'value\s*=\s*["\']([^"\']+)["\']', attrs, re.IGNORECASE
        )
        if not value_match:
            continue

        value  = value_match.group(1)
        parts  = value.split("-")
        if len(parts) < 2:
            continue

        field_id   = parts[-1]   # último segmento = field ID
        is_checked = bool(re.search(r'\bchecked\b', attrs, re.IGNORECASE))

        # Label: texto após o input, ou fallback para option_id
        label = label_text.strip() if label_text.strip() else parts[0]

        if is_checked:
            if field_id not in checked_by_field:
                checked_by_field[field_id] = []
            checked_by_field[field_id].append(label)
        else:
            unchecked_by_field[field_id] = unchecked_by_field.get(field_id, 0) + 1

    # ── Passo 3: Converter field_id → normalized_name ──
    result: Dict[str, List[str]] = {}
    for fid, labels in checked_by_field.items():
        fname = field_id_to_name.get(fid, fid)
        norm  = normalize(fname)
        result[norm] = labels

    if result:
        total_checked   = sum(len(v) for v in result.values())
        total_unchecked = sum(unchecked_by_field.values())
        logger.info(
            "📖 [PATCH-C] ORIGEM HTML: %d campos | %d checked | %d unchecked",
            len(result), total_checked, total_unchecked,
        )
        for norm_name, opts in result.items():
            fname = next(
                (fn for fid, fn in field_id_to_name.items()
                 if normalize(fn) == norm_name),
                norm_name,
            )
            fid_for_unchecked = next(
                (fid for fid, fn in field_id_to_name.items()
                 if normalize(fn) == norm_name),
                "",
            )
            unchecked = unchecked_by_field.get(fid_for_unchecked, 0)
            logger.info(
                "    '%s': %d checked, %d unchecked → %s",
                fname, len(opts), unchecked, opts[:10],
            )
    else:
        logger.warning(
            "⚠️ [PATCH-C] Nenhuma opção checked encontrada no HTML da ORIGEM"
        )
        total_cb = len(re.findall(r"option_info", html))
        logger.info(
            "    (encontrados %d refs a option_info no HTML)", total_cb
        )

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Leitura de infos vinculadas do DESTINO (sem alterações)
# ══════════════════════════════════════════════════════════════════════════════

def get_product_current_infos(
    page: Page, product_id: str, logger
) -> List[str]:
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
            logger.warning(
                "GET infos do produto %s: status %d", product_id, resp.status
            )
            return []

        raw_html = resp.body()
        try:
            html = raw_html.decode("utf-8")
        except UnicodeDecodeError:
            html = raw_html.decode("latin-1", errors="ignore")

        current_ids = set()

        # selected_items[] (qualquer ordem de atributos)
        for m in re.finditer(
            r'<input[^>]*name\s*=\s*["\']selected_items\[\]["\'][^>]*value\s*=\s*["\'](\d+)["\']',
            html, re.DOTALL | re.IGNORECASE,
        ):
            current_ids.add(m.group(1))
        for m in re.finditer(
            r'<input[^>]*value\s*=\s*["\'](\d+)["\'][^>]*name\s*=\s*["\']selected_items\[\]["\']',
            html, re.DOTALL | re.IGNORECASE,
        ):
            current_ids.add(m.group(1))

        # checked + value + selected
        for m in re.finditer(
            r'<input[^>]*\bchecked\b[^>]*value\s*=\s*["\'](\d+)["\'][^>]*name\s*=\s*["\']selected[^"\']*["\']',
            html, re.DOTALL | re.IGNORECASE,
        ):
            current_ids.add(m.group(1))
        for m in re.finditer(
            r'<input[^>]*value\s*=\s*["\'](\d+)["\'][^>]*\bchecked\b[^>]*name\s*=\s*["\']selected[^"\']*["\']',
            html, re.DOTALL | re.IGNORECASE,
        ):
            current_ids.add(m.group(1))

        # sort[] 
        for m in re.finditer(
            r'name\s*=\s*["\']sort\[\]["\'][^>]*value\s*=\s*["\'](\d+)',
            html, re.DOTALL | re.IGNORECASE,
        ):
            current_ids.add(m.group(1))
        for m in re.finditer(
            r'value\s*=\s*["\'](\d+)["\'][^>]*name\s*=\s*["\']sort\[\]["\']',
            html, re.DOTALL | re.IGNORECASE,
        ):
            current_ids.add(m.group(1))

        result = sorted(current_ids)
        logger.info(
            "📎 Produto %s: %d infos vinculadas: %s",
            product_id, len(result), result,
        )
        return result

    except Exception as exc:
        logger.warning(
            "Erro ao buscar infos atuais do produto %s: %s", product_id, exc
        )
        return []


def get_product_current_checked_options(
    page: Page, product_id: str, logger
) -> Dict[str, List[str]]:
    """
    Lê quais opções estão checked no DESTINO para comparação.
    Usa a mesma lógica de parse que a ORIGEM.
    """
    url = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    try:
        resp = page.request.get(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        if resp.status != 200:
            return {}

        raw = resp.body()
        try:
            html = raw.decode("utf-8")
        except UnicodeDecodeError:
            html = raw.decode("latin-1", errors="ignore")

        return _parse_origin_html_options(html, logger)
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# POST de infos adicionais — CORRIGIDO (sem novas alterações nesta versão)
# ══════════════════════════════════════════════════════════════════════════════

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
    except Exception as e:
        _logger.warning(
            "⚠️ Nav p/ infos produto %s falhou: %s — tentando POST",
            product_id, str(e)[:80],
        )

    # Apenas UM _method=POST (fix anterior mantido)
    parts = ["_method=POST"]

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

    # option_info[] controla quais opções individuais ficam checked
    if option_info_entries:
        for entry in option_info_entries:
            parts.append(f"option_info%5B%5D={entry}")

    body     = "&".join(parts)
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
                        finalUrl: resp.url,
                        snippet: (await resp.text()).substring(0, 300),
                    };
                } catch(e) { return {status: 0, error: e.message}; }
            }
            """,
            [endpoint, body],
        )

        status    = int(result.get("status") or 0)
        redirected = bool(result.get("redirected"))
        final_url  = str(result.get("finalUrl") or "")
        snippet    = str(result.get("snippet") or "")
        snippet_lower   = snippet.lower()
        final_url_lower = final_url.lower()

        redirected_to_login = any(
            p in final_url_lower
            for p in ["/admin/login", "/mvc/adm/login", "/adm/login", "/login?", "/login#"]
        )
        login_in_html = any(
            p in snippet_lower
            for p in ['type="password"', "type='password'", 'name="password"', "name='password'"]
        )

        ok = (
            (status in (200, 302) or bool(result.get("ok")))
            and not redirected_to_login
            and not login_in_html
        )
        detail = f"status={status}, redirected={redirected}, final={final_url}"

        if not ok and not snippet.strip():
            detail += ", empty-response"
        if redirected_to_login or login_in_html:
            detail += ", REDIRECT_LOGIN"
            _logger.error(
                "🚨 Sessão expirada! POST produto %s → login", product_id
            )

        return ok, detail
    except Exception as exc:
        return False, str(exc)


# ══════════════════════════════════════════════════════════════════════════════
# Variações, Propriedades (sem alterações além de logs extras)
# ══════════════════════════════════════════════════════════════════════════════

def get_destino_variants(
    page: Page, product_id: str, token: str, logger
) -> Tuple[list, dict]:
    all_variants = []
    page_num     = 1

    while True:
        url = (
            f"{DESTINO_BASE}/admin/api/products/{product_id}/variants"
            f"?sort=order&page[size]=25&page[number]={page_num}"
        )
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                logger.warning(
                    "GET variants %s pág %d: status %d",
                    product_id, page_num, resp.status,
                )
                break
            data  = resp.json()
            items = data.get("data", [])
            if not items:
                break
            all_variants.extend(items)
            paging      = data.get("paging", {})
            total       = paging.get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.5))
        except Exception as exc:
            logger.warning("Erro GET variants pág %d: %s", page_num, exc)
            break

    logger.info(
        "📦 Produto %s: %d variações no DESTINO", product_id, len(all_variants)
    )
    return all_variants, {}


def get_destino_properties(
    page: Page, token: str, logger
) -> Dict[str, str]:
    prop_map: Dict[str, str] = {}
    page_num = 1
    while True:
        url = (
            f"{DESTINO_BASE}/admin/api/properties"
            f"?sort=id&page[size]=25&page[number]={page_num}"
        )
        try:
            resp = page.request.get(url, headers=api_headers(token))
            if resp.status != 200:
                break
            data  = resp.json()
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                name    = (item.get("name") or "").strip()
                prop_id = item.get("id")
                if name and prop_id:
                    prop_map[normalize(name)] = str(prop_id)
            total       = data.get("paging", {}).get("total", 0)
            total_pages = max(1, (total + 24) // 25)
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.5))
        except Exception as exc:
            logger.warning("Erro GET properties pág %d: %s", page_num, exc)
            break
    logger.info("🏷️ Propriedades DESTINO: %d", len(prop_map))
    return prop_map


def get_property_values(
    page: Page, property_id: str, token: str, logger
) -> Dict[str, str]:
    url = f"{DESTINO_BASE}/admin/api/properties/{property_id}"
    try:
        resp = page.request.get(url, headers=api_headers(token))
        if resp.status != 200:
            return {}
        data      = resp.json()
        prop_data = data.get("data", {})
        values: Dict[str, str] = {}
        for pv in (prop_data.get("PropertyValues") or []):
            name = (pv.get("name") or "").strip()
            vid  = pv.get("id")
            if name and vid:
                values[normalize(name)] = str(vid)
        return values
    except Exception as exc:
        logger.warning(
            "Erro GET property %s values: %s", property_id, exc
        )
        return {}


def append_property_value(
    page: Page, property_id: str, value_name: str, token: str, logger
) -> Optional[str]:
    url     = f"{DESTINO_BASE}/admin/api/properties/{property_id}/append-values"
    payload = {"data": {"name": value_name}}
    try:
        resp = page.request.post(
            url=url,
            data=json.dumps(payload),
            headers=api_headers(token),
        )
        if resp.status == 200:
            data = resp.json()
            for pv in reversed(data.get("data", {}).get("PropertyValues", [])):
                if normalize(pv.get("name", "")) == normalize(value_name):
                    vid = str(pv["id"])
                    logger.info("    ✅ Valor '%s' → ID %s", value_name, vid)
                    return vid
            pvs = data.get("data", {}).get("PropertyValues", [])
            if pvs:
                return str(pvs[-1]["id"])
        else:
            body = ""
            try:
                body = resp.text()[:300]
            except Exception:
                pass
            logger.error(
                "    ❌ append-values: status %d — %s", resp.status, body
            )
    except Exception as exc:
        logger.error("    ❌ append-values erro: %s", exc)
    return None


def delete_variant(
    page: Page, variant_id: str, token: str, logger
) -> bool:
    url = f"{DESTINO_BASE}/admin/api/products-variants/{variant_id}"
    try:
        resp = page.request.delete(url, headers=api_headers(token))
        if resp.status == 204:
            logger.info("    🗑️ Variação %s deletada", variant_id)
            return True
        logger.warning(
            "    ❌ DELETE variação %s: status %d", variant_id, resp.status
        )
        return False
    except Exception as exc:
        logger.error("    ❌ DELETE variação %s erro: %s", variant_id, exc)
        return False


def put_variants(
    page: Page, product_id: str, variants_payload: list, token: str
) -> Tuple[bool, int, str]:
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


def post_variants(
    page: Page, product_id: str, variants_payload: list, token: str
) -> Tuple[bool, int, str]:
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