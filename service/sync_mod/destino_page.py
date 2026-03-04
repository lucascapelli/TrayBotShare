# ========================== destino_page.py (VERSÃO V5 - MATCHING BATCH) ==========================
import os
import re
import unicodedata
from difflib import SequenceMatcher
from typing import List, Optional, Tuple, Dict, Any
from patchright.sync_api import Page

from .config import DESTINO_BASE

ENCONTRADOS_PATH = os.path.join("produtos", "Encontrados.txt")
NAO_ENCONTRADOS_PATH = os.path.join("produtos", "NaoEncontrados.txt")


def _append_live_result(file_path: str, nome: str) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(f"{nome.strip()}\n")
            f.flush()
    except Exception:
        pass


def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in nfkd if not unicodedata.combining(c))
    name = re.sub(r'\s+', ' ', name.lower().strip())
    name = re.sub(r'\s+f\s+\d+\.?\d*', '', name)
    name = re.sub(r'\s+colar com nome.*$', '', name)
    name = re.sub(r'\s+banho de ouro$', '', name)
    name = re.sub(r'\s+banho de rhodium$', '', name)
    return name.strip()


def names_match(name1: str, name2: str) -> bool:
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if n1 == n2 or (len(n1) > 10 and (n1 in n2 or n2 in n1)):
        return True
    return SequenceMatcher(None, n1, n2).ratio() > 0.87


def _pick_best_name_candidate(origem_nome: str, candidates: List[dict]) -> Optional[dict]:
    if not candidates:
        return None

    origem_norm = normalize_name(origem_nome)

    for item in candidates:
        item_name = (item.get("name") or "").strip()
        if normalize_name(item_name) == origem_norm:
            return item

    best = None
    best_score = 0.0
    for item in candidates:
        item_name = (item.get("name") or "").strip()
        score = SequenceMatcher(None, origem_norm, normalize_name(item_name)).ratio()
        if score > best_score:
            best_score = score
            best = item

    if best and best_score >= 0.90:
        return best
    return None


# ====================== FUNÇÃO AUXILIAR (mesma do run_sync) ======================
def _origem_product_key(produto: dict) -> str:
    if not isinstance(produto, dict) or not produto:
        return "invalid:none"
    produto_id = str(produto.get("produto_id") or produto.get("id") or "").strip()
    if produto_id and produto_id.isdigit() and produto_id != "0":
        return f"id:{produto_id}"
    referencia = str(produto.get("reference") or produto.get("referencia") or produto.get("sku") or "").strip()
    if referencia:
        return f"ref:{referencia.lower()}"
    nome = str(produto.get("nome") or "").strip()
    if nome:
        nome_lower = nome.lower()[:80]
        nome_hash = str(abs(hash(nome_lower)))[:10]
        return f"nome:{nome_lower}|{nome_hash}"
    return f"hash:{id(produto)}"


# ====================== MATCHING V5 - BATCH (o coração da velocidade) ======================
def match_products_inteligente(
    page: Page,
    origem_products: List[dict],
    destino_cache: Dict[str, Any],
    logger,
    short_delay,
) -> List[dict]:
    """Matching em 3 camadas - faz tudo de uma vez (99% cache)"""
    matches = []
    origem_by_key = {_origem_product_key(p): p for p in origem_products}

    logger.info("🔍 MATCHING V5 - Iniciando (cache + browser apenas misses)...")

    # Camada 1 e 2: Cache (ref/sku → nome)
    for key, produto in list(origem_by_key.items()):
        nome = (produto.get("nome") or "").strip()
        ref = str(produto.get("reference") or produto.get("referencia") or produto.get("sku") or "").strip().lower()
        norm = normalize_name(nome)

        if ref and f"ref:{ref}" in destino_cache:
            data = destino_cache[f"ref:{ref}"]
            matches.append({"destino_id": data["id"], "destino_name": data["name"], "origem_product": produto})
            _append_live_result(ENCONTRADOS_PATH, nome)
            origem_by_key.pop(key, None)
            continue
        if ref and f"sku:{ref}" in destino_cache:
            data = destino_cache[f"sku:{ref}"]
            matches.append({"destino_id": data["id"], "destino_name": data["name"], "origem_product": produto})
            _append_live_result(ENCONTRADOS_PATH, nome)
            origem_by_key.pop(key, None)
            continue
        name_key = f"name:{norm}"
        if name_key in destino_cache:
            cached = destino_cache[name_key]
            candidates = cached if isinstance(cached, list) else [cached]
            data = _pick_best_name_candidate(nome, candidates)
            if data:
                matches.append({"destino_id": data["id"], "destino_name": data["name"], "origem_product": produto})
                _append_live_result(ENCONTRADOS_PATH, nome)
                origem_by_key.pop(key, None)
                continue

    logger.info(f"✅ {len(matches)} produtos encontrados via CACHE")

    # Camada 3: Browser só nos que sobraram (raro)
    if origem_by_key:
        logger.info(f"⚠️ {len(origem_by_key)} produtos indo para busca no browser...")
        browser_matches = _browser_search_batch(page, list(origem_by_key.values()), logger, short_delay)
        matches.extend(browser_matches)

    return matches


def _browser_search_batch(page: Page, pending: List[dict], logger, short_delay) -> List[dict]:
    """Busca em lote - abre a página UMA ÚNICA VEZ"""
    matches = []
    try:
        base_url = f"{DESTINO_BASE}/admin/products/list?sort=name&page[size]=25&page[number]=1"
        page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
        search_box = page.get_by_role("textbox", name="Buscar por nome, código,")

        for produto in pending:
            nome = (produto.get("nome") or "").strip()
            if not nome:
                continue
            try:
                search_box.clear()
                search_box.fill(nome[:60])
                with page.expect_response(
                    lambda r: r.status == 200 and "application/json" in (r.headers.get("content-type") or ""),
                    timeout=10000,
                ) as resp_info:
                    page.keyboard.press("Enter")
                data = resp_info.value.json()
                for item in data.get("data", []):
                    item_name = (item.get("name") or "").strip()
                    if names_match(nome, item_name):
                        matches.append({
                            "destino_id": str(item.get("id")),
                            "destino_name": item_name,
                            "origem_product": produto
                        })
                        _append_live_result(ENCONTRADOS_PATH, nome)
                        logger.info(f"✅ BROWSER MATCH: {nome[:60]}")
                        break
                else:
                    _append_live_result(NAO_ENCONTRADOS_PATH, nome)
            except Exception:
                _append_live_result(NAO_ENCONTRADOS_PATH, nome)
            short_delay()
    except Exception as e:
        logger.error(f"Erro no batch search: {e}")
    return matches


# ====================== FUNÇÕES ANTIGAS MANTIDAS (compatibilidade) ======================
def _extract_destino_token(page: Page) -> str:
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


def fetch_product_and_token(page: Page, product_id: str, logger) -> Tuple[Optional[dict], Optional[str]]:
    # (seu código original mantido exatamente igual - não alterei)
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

    # fallback token + GET API
    if not auth_token:
        try:
            auth_token = page.evaluate(
                """
                (() => {
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
                })()
                """
            )
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
        except Exception as exc:
            logger.warning("Erro GET API %s: %s", product_id, exc)

    return detail_json, auth_token