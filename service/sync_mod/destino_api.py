import json
import logging
import random
import re
import time
from typing import Dict, List, Optional, Tuple
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
            name = c.get("name") or ""
            value = c.get("value") or ""
            if name:
                parts.append(f"{name}={value}")
        elif isinstance(c, str):
            # Já é "name=value" ou "name=value; name2=value2"
            parts.append(c)

    return "; ".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# Product CRUD
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# Additional Info Catalog
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# LEITURA DE OPÇÕES MARCADAS DA ORIGEM VIA HTTP
# (Não usa browser/navegação — faz request HTTP direto com cookies)
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

    Retorna: { normalized_field_name: [checked_option_labels] }
    Exemplo: { "aro": ["09", "12", "13", "14", ...] }
    """
    url = (
        f"{origin_base}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )

    cookie_str = _build_cookie_header(cookies_origem)
    if not cookie_str:
        logger.warning("⚠️ Sem cookies da ORIGEM — não é possível ler opções checked")
        return {}

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cookie": cookie_str,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Referer": f"{origin_base}/admin/products/{product_id}/edit",
    }

    try:
        resp = page.request.get(url, headers=headers)
        if resp.status != 200:
            logger.warning("⚠️ GET opções ORIGEM produto %s: status %d", product_id, resp.status)
            return {}

        raw = resp.body()
        try:
            html = raw.decode("utf-8")
        except UnicodeDecodeError:
            html = raw.decode("latin-1", errors="ignore")

        if len(html) < 200:
            logger.warning("⚠️ HTML da ORIGEM muito curto (%d bytes) — pode ser redirect/login", len(html))
            return {}

        # Verificar se caiu na página de login
        html_lower = html.lower()
        if 'type="password"' in html_lower or "name='password'" in html_lower:
            logger.warning("⚠️ ORIGEM redirecionou para login — cookies inválidos/expirados")
            return {}

        return _parse_origin_html_options(html, logger)

    except Exception as exc:
        logger.warning("⚠️ Erro HTTP lendo opções ORIGEM produto %s: %s", product_id, exc)
        return {}


def _parse_origin_html_options(html: str, logger) -> Dict[str, List[str]]:
    """
    Parseia HTML da página additional_product_info da ORIGEM.

    Estrutura HTML esperada (Tray Commerce):
        Seção de um campo (ex: "993-Aro" ou "135-Aro"):
            <input type="checkbox" class="options_135" name="option_info[]"
                   value="789-135" checked> 12
            <input type="checkbox" class="options_135" name="option_info[]"
                   value="790-135"> 13     ← NÃO marcado

    Retorna: { normalized_field_name: [labels_checked] }
    """
    # ── Passo 1: Mapear field_id → field_name ──
    # Procura textos como "135-Aro", "993-Aro" etc.
    field_id_to_name: Dict[str, str] = {}
    for m in re.finditer(r'(\d{2,6})\s*[-–]\s*([^<"\n\r]{2,60})', html):
        fid = m.group(1)
        fname = m.group(2).strip().rstrip(':').strip()
        if fname and fid not in field_id_to_name:
            field_id_to_name[fid] = fname

    # ── Passo 2: Encontrar todos os checkboxes option_info ──
    # Captura: <input ... > LABEL_TEXT
    checked_by_field: Dict[str, List[str]] = {}
    unchecked_by_field: Dict[str, int] = {}

    for match in re.finditer(
        r'<input\b([^>]*)>([^<]{0,120})',
        html,
        re.IGNORECASE | re.DOTALL,
    ):
        attrs = match.group(1)
        label_text = match.group(2).strip()

        # Só checkboxes de option_info
        if not re.search(r'type\s*=\s*["\']checkbox["\']', attrs, re.IGNORECASE):
            continue
        if "option_info" not in attrs:
            continue

        # Extrair value (formato: OPTION_ID-FIELD_ID)
        value_match = re.search(r'value\s*=\s*["\']([^"\']+)["\']', attrs, re.IGNORECASE)
        if not value_match:
            continue

        value = value_match.group(1)
        parts = value.split("-")
        if len(parts) < 2:
            continue

        field_id = parts[-1]  # último segmento = field ID
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
        norm = normalize(fname)
        result[norm] = labels

    if result:
        total_checked = sum(len(v) for v in result.values())
        total_unchecked = sum(unchecked_by_field.values())
        logger.info(
            "📖 ORIGEM HTML: %d campos | %d checked | %d unchecked",
            len(result), total_checked, total_unchecked,
        )
        for name, opts in result.items():
            fname = field_id_to_name.get(name, name)
            unchecked = unchecked_by_field.get(
                # Encontrar o field_id original para este nome
                next((fid for fid, fn in field_id_to_name.items() if normalize(fn) == name), ""),
                0,
            )
            logger.info("    '%s': %d checked, %d unchecked → %s", fname, len(opts), unchecked, opts[:10])
    else:
        logger.warning("⚠️ Nenhuma opção checked encontrada no HTML da ORIGEM")
        # Debug: mostrar se encontrou algum checkbox
        total_cb = len(re.findall(r'option_info', html))
        logger.info("    (encontrados %d referências a option_info no HTML)", total_cb)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Leitura de infos vinculadas do DESTINO (quais CAMPOS estão vinculados)
# ══════════════════════════════════════════════════════════════════════════════

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
        logger.info("📎 Produto %s: %d infos vinculadas: %s", product_id, len(result), result)
        return result

    except Exception as exc:
        logger.warning("Erro ao buscar infos atuais do produto %s: %s", product_id, exc)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Leitura de opções CHECKED do DESTINO (para comparação)
# ══════════════════════════════════════════════════════════════════════════════

def get_product_current_checked_options(page: Page, product_id: str, logger) -> Dict[str, List[str]]:
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
# POST de infos adicionais — CORRIGIDO
# FIX: _method duplicado + login detection + option_info controlado
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

    # FIX: Apenas UM _method=POST
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
                        finalUrl: resp.url,
                        snippet: (await resp.text()).substring(0, 300),
                    };
                } catch(e) { return {status: 0, error: e.message}; }
            }
            """,
            [endpoint, body],
        )

        status = int(result.get("status") or 0)
        redirected = bool(result.get("redirected"))
        final_url = str(result.get("finalUrl") or "")
        snippet = str(result.get("snippet") or "")
        snippet_lower = snippet.lower()
        final_url_lower = final_url.lower()

        redirected_to_login = any(p in final_url_lower for p in [
            "/admin/login", "/mvc/adm/login", "/adm/login", "/login?", "/login#",
        ])
        login_in_html = any(p in snippet_lower for p in [
            'type="password"', "type='password'", 'name="password"', "name='password'",
        ])

        ok = (status in (200, 302) or bool(result.get("ok"))) and not redirected_to_login and not login_in_html
        detail = f"status={status}, redirected={redirected}, final={final_url}"

        if not ok and not snippet.strip():
            detail += ", empty-response"
        if redirected_to_login or login_in_html:
            detail += ", REDIRECT_LOGIN"
            _logger.error("🚨 Sessão expirada! POST produto %s → login", product_id)

        return ok, detail
    except Exception as exc:
        return False, str(exc)


# ══════════════════════════════════════════════════════════════════════════════
# Variações, Propriedades, etc. (sem mudanças)
# ══════════════════════════════════════════════════════════════════════════════

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
        for pv in (prop_data.get("PropertyValues") or []):
            name = (pv.get("name") or "").strip()
            vid = pv.get("id")
            if name and vid:
                values[normalize(name)] = str(vid)
        return values
    except Exception as exc:
        logger.warning("Erro GET property %s values: %s", property_id, exc)
        return {}


def append_property_value(page: Page, property_id: str, value_name: str, token: str, logger) -> Optional[str]:
    url = f"{DESTINO_BASE}/admin/api/properties/{property_id}/append-values"
    payload = {"data": {"name": value_name}}
    try:
        resp = page.request.post(url=url, data=json.dumps(payload), headers=api_headers(token))
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
            logger.error("    ❌ append-values: status %d — %s", resp.status, body)
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
        resp = page.request.put(url=url, data=json.dumps({"data": variants_payload}), headers=api_headers(token))
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
        resp = page.request.post(url=url, data=json.dumps({"data": variants_payload}), headers=api_headers(token))
        body = ""
        try:
            body = resp.text()[:500]
        except Exception:
            pass
        return resp.ok, resp.status, body
    except Exception as exc:
        return False, 0, str(exc)