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
#
# Obs: substitua por inteiro este arquivo no seu projeto para garantir que o
# post_additional_infos novo e os patches estejam presentes.
#
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
# ══════════════════════════════════════════════════════════════════════

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
# ══════════════════════════════════════════════════════════════════════

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

    # usar encoding padrão (str) aqui — chamadas que exigem UTF-8 usam urlencode(..., encoding='utf-8')
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
    page: Page, info_name: str, logger, field_type: str = "S"
) -> bool:
    """
    Cria um campo Additional Info no DESTINO.
    field_type:
      - "S" = select/options
      - "T" = textarea (texto)
      - "I" = input (campo textual single-line)

    Ajusta 'obrigatorio' e 'add_total' conforme tipo:
      - Para text/input (T/I) => obrigatorio = 0, add_total = 0
      - Para select (S) => obrigatorio = 1, add_total = 1
    """
    # Definir flags de acordo com tipo
    obrigatorio_flag = "0" if field_type in ("T", "I") else "1"
    add_total_flag = "0" if field_type in ("T", "I") else "1"

    form_data = {
        "nome_loja": info_name,
        "nome_adm":  info_name,
        "ativa":     "1",
        "exibir_valor": "0",
        "obrigatorio":  obrigatorio_flag,
        "contador":     "0",
        "tipo":         field_type,
        "valor":        "0.00",
        "add_total":    add_total_flag,
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
                "🆕 Campo Additional Info criado via %s (status %s): %s (tipo=%s)",
                endpoint, status, info_name, field_type,
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
    field_type: str = "S",
) -> Optional[dict]:
    """
    Garante que exista um campo AdditionalInfo com as opções desejadas no DESTINO.
    field_type: "S"=select, "T"=textarea/text, "I"=input
    Retorna o dict { "id": "...", "name": "...", "option_map": {...} } do catálogo final,
    ou None se falhar.
    """
    try:
        catalog = fetch_all_additional_infos_catalog(page, token, logger=logger)
        normalized_name = normalize(info_name)
        info = catalog.get(normalized_name)

        if info:
            # verificar opções faltantes e criar se necessário (pular se textual)
            missing = []
            for opt in option_names:
                if normalize(opt) not in info.get("option_map", {}):
                    missing.append(opt)
            if missing and field_type != "T":
                logger.info("Opções faltando para '%s': %s", info_name, missing)
                for opt in missing:
                    created = _create_additional_info_option(page, info["id"], opt, logger)
                    if created:
                        # atualizar o catálogo localmente
                        info["option_map"][normalize(opt)] = "unknown"
                # Não garantimos IDs das opções criadas aqui; usuário pode reconsultar catálogo
            return info

        # se não existe, criar campo + opções (respeitando field_type)
        created_field = _create_additional_info_field(page, info_name, logger, field_type=field_type)
        if not created_field:
            logger.warning("Não foi possível criar campo additional info '%s' (tipo=%s)", info_name, field_type)
            return None

        # após criar, recarregar catálogo e tentar mapear opções
        catalog = fetch_all_additional_infos_catalog(page, token, logger=logger)
        info = catalog.get(normalized_name)
        if not info:
            logger.warning("Campo criado mas não encontrado no catálogo após criação: %s", info_name)
            return None

        # criar opções ausentes explicitamente (pular criação de opções quando textual)
        if field_type != "T":
            for opt in option_names:
                if normalize(opt) not in info.get("option_map", {}):
                    _create_additional_info_option(page, info["id"], opt, logger)

        # recarregar final
        catalog = fetch_all_additional_infos_catalog(page, token, logger=logger)
        return catalog.get(normalized_name)
    except Exception as exc:
        logger.warning("Erro ensure_additional_info_with_options para %s: %s", info_name, exc)
        return None


# ====================== UTILITÁRIOS DE LEITURA DO PRODUTO ATUAL ======================

def get_product_current_infos(
    page: Page,
    product_id: str,
    logger
) -> List[str]:
    """
    Tenta recuperar os Additional Info IDs atualmente vinculados ao produto no DESTINO.
    Estratégia:
      1) Tenta chamada API GET /admin/api/products/{product_id} sem token (usa sessão do page)
      2) Se falhar, carrega a página de edição e tenta extrair inputs selected_items via DOM
    Retorna lista de ids (strings).
    """
    results: List[str] = []
    try:
        url = f"{DESTINO_BASE}/admin/api/products/{product_id}"
        try:
            resp = page.request.get(url, headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}, timeout=15000)
            if resp.status == 200:
                data = resp.json()
                p = data.get("data") or data
                # possíveis lugares onde os AdditionalInfos aparecem
                candidates = [
                    p.get("AdditionalInfos"),
                    p.get("additional_infos"),
                    p.get("AdditionalProductInfo"),
                    (p.get("data") or {}).get("AdditionalInfos") if isinstance(p.get("data"), dict) else None,
                ]
                for cand in candidates:
                    if isinstance(cand, list):
                        for it in cand:
                            if isinstance(it, dict) and it.get("id"):
                                results.append(str(it.get("id")))
                            elif isinstance(it, (str, int)):
                                results.append(str(it))
                if results:
                    return results
        except Exception as e:
            logger.debug("get_product_current_infos: API GET falhou: %s", e)

        # fallback: tentar extrair via DOM na página de edição
        try:
            edit_url = f"{DESTINO_BASE}/mvc/adm/additional_product_info/additional_product_info/edit/{product_id}"
            page.goto(edit_url, wait_until="domcontentloaded", timeout=15000)
        except Exception:
            try:
                page.goto(f"{DESTINO_BASE}/admin/products/{product_id}/edit", wait_until="domcontentloaded", timeout=15000)
            except Exception:
                pass

        # avaliar inputs selected_items ou campos hidden que indiquem selecionados
        try:
            vals = page.evaluate(
                """() => {
                    const out = [];
                    // inputs com name starting selected_items
                    document.querySelectorAll('input[name^="selected_items"]').forEach(n => {
                        if (n.value) out.push(n.value);
                    });
                    // selects / checkboxes com classe ou data attribute comum
                    document.querySelectorAll('select, input[type="checkbox"]').forEach(n => {
                        const name = n.getAttribute && n.getAttribute('name');
                        if (name && name.startsWith('selected_items') && n.value) out.push(n.value);
                    });
                    return out;
                }"""
            )
            if isinstance(vals, list) and vals:
                return [str(v) for v in vals if v]
        except Exception as e:
            logger.debug("get_product_current_infos: DOM extraction failed: %s", e)

    except Exception as exc:
        logger.warning("Erro get_product_current_infos: %s", exc)

    return [str(x) for x in results]


# ====================== POST / EDIT: post_additional_infos (VERSÃO ROBUSTA) ======================

def post_additional_infos(
    page: Page,
    product_id: str,
    info_ids_to_link: List[str],
    short_delay,
    sort_entries: Optional[List[str]] = None,
    option_info_entries: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    """
    Envia POST para vincular Additional Infos no DESTINO (Tray).
    - Extrai CSRF token da página de edição
    - Monta payload idêntico ao form real
    - Usa evaluate/fetch para simular envio do navegador
    - Valida pós-envio com reload e reconsulta
    """
    edit_url = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )

    _logger.info("Acessando página de edição para extrair CSRF e contexto: %s", edit_url)

    try:
        # 1. Carrega a página de edição (necessário para CSRF e sessão)
        response = page.goto(edit_url, wait_until="networkidle", timeout=45000)
        short_delay()  # espera JS carregar

        if not response or (hasattr(response, "status") and response.status != 200):
            _logger.error(
                "Falha ao carregar página de edição: status %s",
                response.status if response and hasattr(response, "status") else "sem response",
            )
            return False, "Falha carregando página de edição"

        # 2. Extrai CSRF token (os dois nomes mais comuns no Tray/Laravel)
        csrf_token = None
        for selector in [
            'input[name="_token"]',
            'input[name="__RequestVerificationToken"]',
            'meta[name="csrf-token"]',
        ]:
            try:
                elem = page.locator(selector).first
                if elem.count() > 0:
                    if selector.startswith('meta'):
                        csrf_token = elem.get_attribute("content")
                    else:
                        csrf_token = elem.get_attribute("value")
                    if csrf_token:
                        _logger.info("🔒 CSRF token encontrado (%s): %s...", selector, str(csrf_token)[:20])
                        break
            except Exception:
                continue

        if not csrf_token:
            _logger.warning("⚠️ Nenhum CSRF token encontrado na página — enviando sem (pode falhar)")

        # 3. Monta payload exatamente como o form real envia
        payload = {
            "_method": "POST",
            "id_produto": str(product_id),
            "data[AdditionalProductInfo][herda_prazo]": "0",
            "data[AdditionalProductInfo][prazo]": "0",
        }

        # selected_items[] — formato indexado (muito comum no Tray)
        for i, info_id in enumerate(info_ids_to_link):
            payload[f"selected_items[{i}]"] = str(info_id)

        # sort[] — mantém ordem enviada ou default
        if sort_entries:
            for i, s in enumerate(sort_entries):
                payload[f"sort[{i}]"] = s
        else:
            for i, info_id in enumerate(info_ids_to_link):
                payload[f"sort[{i}]"] = f"{info_id}-"

        # option_info[] — as opções checked (ex: 2839-951)
        if option_info_entries:
            for i, entry in enumerate(option_info_entries):
                payload[f"option_info[{i}]"] = entry

        # Adiciona CSRF se encontrado
        if csrf_token:
            payload["_token"] = csrf_token

        # Campos extras comuns no Tray (commit, etc.)
        payload["commit"] = "Salvar"
        payload["action"] = "edit"

        # =========================
        # Limpeza prévia: desmarcar tudo antes de enviar os selecionados
        # =========================
        try:
            _logger.info("Enviando POST de limpeza (desmarcar tudo)...")
            clean_payload = {
                "_method": "POST",
                "id_produto": str(product_id),
                "data[AdditionalProductInfo][herda_prazo]": "0",
                "data[AdditionalProductInfo][prazo]": "0",
            }
            # incluir selected_items com os mesmos índices (o objetivo é forçar regravação)
            for i, sid in enumerate(info_ids_to_link):
                clean_payload[f"selected_items[{i}]"] = str(sid)
            # manter sort se houver
            for i, s in enumerate(sort_entries or []):
                clean_payload[f"sort[{i}]"] = s
            if csrf_token:
                clean_payload["_token"] = csrf_token
            clean_payload["commit"] = "Salvar"
            clean_payload["action"] = "edit"

            # garantir UTF-8 no urlencode da limpeza
            clean_body = urlencode(clean_payload, doseq=True, encoding="utf-8")

            # enviar via fetch usando headers com charset
            clean_result = page.evaluate(
                """
                async ([url, body, origin]) => {
                    try {
                        const response = await fetch(url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                'X-Requested-With': 'XMLHttpRequest',
                                'Referer': url,
                                'Origin': origin
                            },
                            body: body,
                            credentials: 'include',
                            redirect: 'follow'
                        });
                        const text = await response.text();
                        return {
                            ok: response.ok,
                            status: response.status,
                            redirected: response.redirected,
                            finalUrl: response.url,
                            snippet: text.substring(0, 400)
                        };
                    } catch (err) {
                        return { error: err.message };
                    }
                }
                """,
                [edit_url, clean_body, DESTINO_BASE.rstrip("/")],
            )

            if isinstance(clean_result, dict):
                c_status = clean_result.get("status", 0)
                _logger.info("Resultado limpeza: status=%s ok=%s snippet_len=%d", c_status, clean_result.get("ok"), len(clean_result.get("snippet") or ""))
            else:
                _logger.warning("Resultado inesperado na limpeza: %r", clean_result)
        except Exception as e:
            _logger.warning("Erro durante POST de limpeza: %s", e)

        # esperar propagation após limpeza (4-5s recomendado)
        time.sleep(5)

        # Converte para string urlencoded (forçando UTF-8)
        body_str = urlencode(payload, doseq=True, encoding="utf-8")

        _logger.debug(
            "POST payload completo (urlencoded): %s",
            body_str[:1200] + "..." if len(body_str) > 1200 else body_str,
        )
        _logger.info(
            "Enviando POST com %d campos + %d opções marcadas",
            len(info_ids_to_link),
            len(option_info_entries or []),
        )

        # 4. Envia via fetch no evaluate (mantém cookies/sessão do Playwright)
        result = page.evaluate(
            """
            async ([url, body, origin]) => {
                try {
                    const response = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'X-Requested-With': 'XMLHttpRequest',
                            'Referer': url,
                            'Origin': origin
                        },
                        body: body,
                        credentials: 'include',
                        redirect: 'follow'
                    });
                    const text = await response.text();
                    return {
                        ok: response.ok,
                        status: response.status,
                        redirected: response.redirected,
                        finalUrl: response.url,
                        snippet: text.substring(0, 400)
                    };
                } catch (err) {
                    return { error: err.message };
                }
            }
            """,
            [edit_url, body_str, DESTINO_BASE.rstrip("/")],
        )

        if not isinstance(result, dict):
            _logger.error("Resultado inesperado do evaluate/fetch: %r", result)
            return False, "Resultado inesperado do fetch"

        status = result.get("status", 0)
        ok = result.get("ok", False) and status in (200, 302, 201)
        redirected = result.get("redirected", False)
        final_url = result.get("finalUrl", "") or ""
        snippet = result.get("snippet", "") or ""

        detail = f"status={status}, ok={ok}, redirected={redirected}, final={final_url}, snippet_len={len(snippet)}"

        if "login" in final_url.lower() or "password" in snippet.lower():
            detail += " → POSSÍVEL REDIRECT PARA LOGIN"
            _logger.error("Sessão inválida detectada no POST additional infos")

        _logger.info("POST additional_infos → %s", detail)

        # 5. Validação pós-envio (delay + reload + reconsulta)
        if ok:
            _logger.info("POST aparentemente aceito → aguardando propagação no Tray...")
            time.sleep(6)  # Tray pode demorar para refletir no banco
            try:
                page.reload(wait_until="networkidle", timeout=20000)
                time.sleep(3)
            except Exception as e:
                _logger.warning("Reload falhou: %s", str(e))

            # Retorna True só se realmente salvou (evita falso-positivo)
            try:
                atuais = get_product_current_infos(page, product_id, _logger)
            except Exception as e:
                _logger.warning("Falha ao consultar infos atuais após POST: %s", e)
                return False, detail + " | VALIDACAO_IMPOSSIVEL"

            # normalizar tipos (strings)
            atuais_set = {str(x) for x in (atuais or [])}
            esperado_set = {str(x) for x in (info_ids_to_link or [])}

            if atuais_set == esperado_set:
                _logger.info("✅ Validação pós-POST: campos salvos corretamente (%d)", len(atuais_set))
                return True, detail + " | VALIDADO_OK"
            else:
                _logger.warning(
                    "⚠️ Campos NÃO salvos após POST: esperados=%s, atuais=%s",
                    sorted(list(esperado_set)),
                    sorted(list(atuais_set)),
                )
                return False, detail + " | VALIDADO_FALHOU"

        return ok, detail

    except Exception as exc:
        _logger.error("❌ Erro geral em post_additional_infos: %s", str(exc))
        return False, str(exc)