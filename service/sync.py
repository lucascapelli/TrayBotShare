# service/sync.py
# Sincronização ORIGEM → DESTINO
# Usa busca por nome na barra de pesquisa (mesma técnica do scraperDestino)
# para encontrar matches, depois abre /edit só dos que deram match, faz PUT.
#
# STEALTH: busca no search bar (humano), delays gaussianos, rate limit.

import json
import logging
import os
import re
import time
import random
from typing import Any, Dict, List, Optional, Tuple

from patchright.sync_api import Page

logger = logging.getLogger("sync")

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
RATE_LIMIT = 5          # Máx. de PUTs por execução (alterar quando ok)
DESTINO_BASE = "https://www.grasielyatacado.com.br"
ORIGEM_JSON_PATH = "produtos/ProdutosOrigem.json"


# ---------------------------------------------------------------------------
# Stealth: delays humanos
# ---------------------------------------------------------------------------
def _human_delay(min_s: float = 2.0, max_s: float = 5.0):
    mid = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mid, std)))
    time.sleep(delay)


def _search_delay():
    """Entre buscas no search bar (como se digitasse e lesse resultados)."""
    _human_delay(2.0, 4.0)


def _page_transition_delay():
    """Antes de abrir /edit (como se lesse a lista)."""
    _human_delay(3.0, 6.0)


def _post_put_delay():
    """Após PUT (como se conferisse o resultado)."""
    _human_delay(4.0, 8.0)


# ---------------------------------------------------------------------------
# Carregamento da ORIGEM (local)
# ---------------------------------------------------------------------------
def _normalize_name(name: str) -> str:
    if not name:
        return ""
    return " ".join(name.strip().split()).lower()


def _load_origem_products() -> List[dict]:
    """Carrega ProdutosOrigem.json como lista."""
    if not os.path.isfile(ORIGEM_JSON_PATH):
        logger.error("Arquivo %s não encontrado", ORIGEM_JSON_PATH)
        return []
    try:
        with open(ORIGEM_JSON_PATH, "r", encoding="utf-8") as f:
            produtos = json.load(f)
        logger.info("%d produtos carregados da ORIGEM (local)", len(produtos))
        return produtos
    except Exception as e:
        logger.error("Erro ao carregar ORIGEM: %s", e)
        return []


# ---------------------------------------------------------------------------
# Buscar matches via barra de pesquisa do DESTINO
# (mesma técnica do scraperDestino.collect_matched_ids_via_search)
# ---------------------------------------------------------------------------
def _find_matches_via_search(
    page: Page,
    origem_products: List[dict],
) -> List[dict]:
    """
    Para cada produto da ORIGEM, busca pelo nome na barra de pesquisa do DESTINO.
    Retorna lista de matches: [{"destino_id", "destino_name", "origem_product"}, ...]

    Stealth: simula humano digitando no search, lendo resultados.
    """
    base_list_url = f"{DESTINO_BASE}/admin/products/list?sort=name&page[size]=25&page[number]=1"

    # Garante que estamos na listagem
    try:
        page.goto(base_list_url, wait_until="networkidle", timeout=15000)
    except Exception as e:
        logger.error("Erro ao carregar listagem: %s", e)
        return []

    # Barra de busca (mesmo localizador do scraperDestino)
    search_locator = page.get_by_role("textbox", name="Buscar por nome, código,")

    matches = []
    seen_ids = set()

    for idx, produto in enumerate(origem_products, 1):
        nome_origem = (produto.get("nome") or "").strip()
        if not nome_origem:
            continue

        # Se já atingimos o rate limit em matches, podemos parar de buscar
        if len(matches) >= RATE_LIMIT:
            logger.info("Rate limit de %d matches atingido, parando busca", RATE_LIMIT)
            break

        try:
            # Limpa e preenche a busca
            search_locator.clear()
            search_locator.fill(nome_origem)

            # Intercepta a resposta da API
            with page.expect_response(
                lambda response: (
                    response.status == 200
                    and "application/json" in (response.headers.get("content-type", "") or "")
                    and (
                        "/api/products" in response.url
                        or "products-search" in response.url
                        or "/products/search" in response.url
                    )
                ),
                timeout=10000,
            ) as response_info:
                page.keyboard.press("Enter")

            data = response_info.value.json()

            if data.get("data"):
                for item in data["data"]:
                    pid = str(item.get("id", ""))
                    item_name = (item.get("name") or "").strip()

                    # Match por nome (flexível, mesma lógica do scraperDestino)
                    if pid and (
                        nome_origem.lower() in item_name.lower()
                        or item_name.lower() in nome_origem.lower()
                    ):
                        if pid not in seen_ids:
                            seen_ids.add(pid)
                            matches.append({
                                "destino_id": pid,
                                "destino_name": item_name,
                                "origem_product": produto,
                            })
                            logger.info(
                                "[%d] ✅ Match: '%s' → ID %s",
                                idx, nome_origem[:50], pid,
                            )
                        break  # um match por nome

            # Pausa stealth entre buscas
            _search_delay()

        except Exception as e:
            logger.debug("[%d] Busca falhou para '%s': %s", idx, nome_origem[:40], e)
            _human_delay(1.0, 2.0)
            continue

    logger.info("%d matches encontrados via busca", len(matches))
    return matches


# ---------------------------------------------------------------------------
# Obter JSON completo de um produto (precisa pra pegar seo_preview.link)
# (mesma técnica do scraperDestino.collect_product_data_destino)
# ---------------------------------------------------------------------------
def _fetch_destino_product(page: Page, product_id: str, timeout: int = 12000) -> Tuple[Optional[dict], Optional[str]]:
    """
    Navega até /products/{id}/edit, intercepta JSON e token.
    Retorna (json_data, auth_token) ou (None, None).
    """
    detail_json = None
    auth_token = None

    def _handle_response(response):
        nonlocal detail_json, auth_token
        if detail_json:
            return
        try:
            ct = response.headers.get("content-type", "")
            if "application/json" not in ct:
                return
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                rid = data["data"].get("id")
                if rid is not None and str(rid) == str(product_id):
                    detail_json = data["data"]
                    # Captura o token do request que buscou este produto
                    token = response.request.headers.get("authorization")
                    if token:
                        auth_token = token
        except Exception:
            pass

    page.on("response", _handle_response)
    try:
        page.goto(
            f"{DESTINO_BASE}/admin/products/{product_id}/edit",
            wait_until="domcontentloaded",
            timeout=timeout,
        )
        waited = 0
        while detail_json is None and waited < timeout:
            page.wait_for_timeout(250)
            waited += 250
    except Exception as e:
        logger.warning("Erro ao carregar produto %s: %s", product_id, e)
    finally:
        page.remove_listener("response", _handle_response)

    # Fallback: tentar pegar token do localStorage
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
            if auth_token:
                logger.info("Token obtido via localStorage")
        except Exception:
            pass

    # Garantir prefixo Bearer
    if auth_token and not auth_token.lower().startswith("bearer "):
        auth_token = f"Bearer {auth_token}"

    return detail_json, auth_token


# ---------------------------------------------------------------------------
# Mapeamento de infos adicionais: ORIGEM nome → DESTINO info_id
# ---------------------------------------------------------------------------
def _fetch_destino_info_map(page: Page, auth_token: str) -> Dict[str, str]:
    """
    Busca todas as infos adicionais do DESTINO via API e cria mapa:
    nome_normalizado → destino_id
    
    Ex: "tamanho do aro" → "15"
    """
    api_url = f"{DESTINO_BASE}/admin/api/additional-info"
    headers = {
        "Authorization": auth_token,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }

    info_map = {}  # nome_normalizado → destino_id
    page_num = 1

    while True:
        try:
            url = f"{api_url}?sort=id&page[size]=25&page[number]={page_num}"
            response = page.request.get(url, headers=headers)
            if response.status != 200:
                logger.warning("Falha ao buscar infos adicionais do DESTINO: %d", response.status)
                break
            data = response.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                # Usar custom_name (preferido) ou name
                name = (item.get("custom_name") or item.get("name") or "").strip()
                item_id = item.get("id")
                if name and item_id:
                    key = _normalize_name(name)
                    info_map[key] = str(item_id)

            total = data.get("paging", {}).get("total", 0)
            total_pages = (total + 24) // 25
            if page_num >= total_pages:
                break
            page_num += 1
            time.sleep(random.uniform(0.3, 0.6))

        except Exception as e:
            logger.warning("Erro ao buscar infos adicionais página %d: %s", page_num, e)
            break

    logger.info("Mapa de infos adicionais do DESTINO: %d entradas", len(info_map))
    return info_map


# ---------------------------------------------------------------------------
# Payload PUT
# ---------------------------------------------------------------------------
def _build_put_payload(origem: dict, destino_json: dict) -> dict:
    """
    Mapeia campos do JSON local (ORIGEM) para formato da API do DESTINO.
    Preserva seo_preview.link do DESTINO.
    """
    payload = {}

    field_map = {
        "nome": "name",
        "preco": "price",
        "descricao": "description",
        "estoque": "stock",
        "estoque_minimo": "minimum_stock",
        "categoria": "category_name",
        "referencia": "reference",
        "peso": "weight",
        "altura": "height",
        "largura": "width",
        "comprimento": "length",
        "itens_inclusos": "included_items",
        "mensagem_adicional": "additional_message",
        "tempo_garantia": "warranty",
    }

    for origem_key, destino_key in field_map.items():
        val = origem.get(origem_key)
        if val is not None:
            payload[destino_key] = val

    # Preço: garantir formato "76.20" (string com 2 casas decimais)
    if "price" in payload:
        try:
            payload["price"] = f"{float(payload['price']):.2f}"
        except (ValueError, TypeError):
            pass

    # Booleanos → "1"/"0"
    if "ativo" in origem:
        payload["active"] = "1" if origem["ativo"] else "0"
    if "visivel" in origem:
        payload["visible"] = "1" if origem["visivel"] else "0"
    if "notificacao_estoque_baixo" in origem:
        payload["minimum_stock_alert"] = "1" if origem["notificacao_estoque_baixo"] else "0"

    # SEO: preservar link do DESTINO
    destino_url = destino_json.get("url", {})
    destino_link = destino_url.get("https") if isinstance(destino_url, dict) else None
    if destino_link:
        payload["url"] = {"https": destino_link}

    # Metatags SEO (title e description da ORIGEM)
    origem_seo = origem.get("seo_preview", {})
    metatags = []
    if origem_seo.get("title"):
        metatags.append({"type": "title", "content": origem_seo["title"]})
    if origem_seo.get("description"):
        metatags.append({"type": "description", "content": origem_seo["description"]})
    if metatags:
        payload["metatag"] = metatags

    # Informações adicionais: NÃO vão no PUT.
    # São vinculadas ao produto via POST separado (endpoint PHP).
    # Ver _attach_additional_infos_to_product()

    # Imagem principal
    img_url = origem.get("imagem_url")
    if img_url:
        payload["ProductImage"] = [{"https": img_url}]

    return payload


# ---------------------------------------------------------------------------
# Vincular infos adicionais ao produto (POST separado - endpoint PHP)
# ---------------------------------------------------------------------------
def _attach_additional_infos_to_product(
    page: Page,
    product_id: str,
    origem_infos: list,
    info_map: Dict[str, str],
) -> Tuple[int, int]:
    """
    Vincula infos adicionais ao produto via POST PHP.
    Usa fetch() de dentro da página pra herdar cookies/sessão do browser.
    
    Endpoint: /mvc/adm/additional_product_info/additional_product_info/edit/{product_id}
    Payload exato capturado do browser.
    """
    if not origem_infos or not info_map:
        return 0, 0

    # Coletar IDs do DESTINO que precisam ser vinculados
    destino_info_ids = []
    for info in origem_infos:
        nome_info = (info.get("nome") or "").strip()
        nome_norm = _normalize_name(nome_info)
        destino_id = info_map.get(nome_norm)
        if destino_id:
            destino_info_ids.append(destino_id)
            logger.info("   Info '%s' → DESTINO ID %s", nome_info, destino_id)
        else:
            logger.debug("   Info '%s' não encontrada no DESTINO — pulando", nome_info)

    if not destino_info_ids:
        return 0, 0

    # Navegar pra página de infos adicionais do produto (seta referrer + sessão)
    info_page_url = (
        f"{DESTINO_BASE}/admin/#/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    try:
        page.goto(info_page_url, wait_until="networkidle", timeout=15000)
        _human_delay(1.5, 3.0)
    except Exception as e:
        logger.warning("Erro ao navegar pra página de infos: %s", e)

    # Montar payload EXATO como o browser manda
    # _method=POST aparece DUAS vezes
    # selected_items[] sem índice numérico
    # sort[] sem índice numérico
    # prazo = 0 (não vazio)
    parts = ["_method=POST", "_method=POST"]
    for info_id in destino_info_ids:
        parts.append(f"selected_items%5B%5D={info_id}")
    parts.append(f"id_produto={product_id}")
    parts.append("data%5BAdditionalProductInfo%5D%5Bherda_prazo%5D=0")
    parts.append("data%5BAdditionalProductInfo%5D%5Bprazo%5D=0")
    for info_id in destino_info_ids:
        parts.append(f"sort%5B%5D={info_id}-")
    body = "&".join(parts)

    endpoint = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )

    # Executar fetch DE DENTRO da página (herda cookies + sessão automaticamente)
    try:
        result = page.evaluate("""
            async ([url, body]) => {
                try {
                    const resp = await fetch(url, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                        body: body,
                        redirect: 'follow',
                        credentials: 'include',
                    });
                    const text = await resp.text();
                    return {
                        status: resp.status,
                        ok: resp.ok,
                        redirected: resp.redirected,
                        bodySnippet: text.substring(0, 300),
                    };
                } catch(e) {
                    return {status: 0, error: e.message};
                }
            }
        """, [endpoint, body])

        status = result.get("status", 0)
        ok = result.get("ok", False)
        redirected = result.get("redirected", False)
        error = result.get("error", "")

        # 302 seguido de 200 após redirect = sucesso
        # fetch com redirect:follow transforma 302 → 200
        if ok or status == 200 or status == 302:
            logger.info(
                "Produto %s: %d infos vinculadas (status=%d, redirected=%s)",
                product_id, len(destino_info_ids), status, redirected,
            )
            return len(destino_info_ids), 0
        else:
            snippet = result.get("bodySnippet", "")[:150]
            logger.warning(
                "Produto %s: POST infos falhou (status=%d, error=%s): %s",
                product_id, status, error, snippet,
            )
            return 0, len(destino_info_ids)

    except Exception as e:
        logger.warning("Produto %s: erro no fetch de infos: %s", product_id, e)
        return 0, len(destino_info_ids)


# ---------------------------------------------------------------------------
# Enviar PUT
# ---------------------------------------------------------------------------
def _put_product(page: Page, product_id: str, payload: dict, auth_token: str) -> Tuple[bool, int, str]:
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    if auth_token:
        headers["Authorization"] = auth_token
    try:
        response = page.request.put(
            url=url,
            data=json.dumps({"data": payload}),
            headers=headers,
        )
        body = ""
        try:
            body = response.text()[:400]
        except Exception:
            pass
        return response.ok, response.status, body
    except Exception as e:
        return False, 0, str(e)


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------
def run_sync(
    context: Any,
    storage_origem,
    storage_destino,
    origem_url: str,
    source_user: str,
    source_pass: str,
    cookies_origem: list,
):
    """
    Fluxo:
    1. Carrega ProdutosOrigem.json
    2. Busca cada nome da ORIGEM no search bar do DESTINO (stealth, igual scraperDestino)
    3. Para nos primeiros RATE_LIMIT matches
    4. Abre /edit só desses → pega JSON completo (pra preservar seo_preview.link)
    5. PUT com dados da ORIGEM
    """
    print("\n" + "=" * 70)
    print("🔄 SYNC: ORIGEM → DESTINO")
    print(f"   Rate limit: {RATE_LIMIT} produtos por execução")
    print("=" * 70)

    # 1) Carregar ORIGEM
    origem_products = _load_origem_products()
    if not origem_products:
        print("❌ Nenhum produto na ORIGEM. Encerrando.")
        return

    # Página do contexto DESTINO
    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta no contexto. Encerrando.")
        return
    page = pages[0]

    # 2) Buscar matches via search bar (stealth)
    print(f"\n🔍 ETAPA 1: Buscando matches na barra de pesquisa do DESTINO...")
    print(f"   ({len(origem_products)} nomes da ORIGEM, parando em {RATE_LIMIT} matches)")
    print("-" * 70)

    matches = _find_matches_via_search(page, origem_products)

    if not matches:
        print("❌ Nenhum match encontrado. Verifique se os nomes são iguais entre ORIGEM e DESTINO.")
        return

    # Limitar ao rate limit (já deve estar limitado, mas por segurança)
    to_process = matches[:RATE_LIMIT]

    print(f"\n✅ {len(matches)} matches encontrados")
    print(f"🔒 Processando {len(to_process)} (rate limit = {RATE_LIMIT})\n")

    for i, m in enumerate(to_process, 1):
        print(f"   {i}. [{m['destino_id']}] {m['destino_name'][:65]}")
    print()

    # 3) Buscar mapa de infos adicionais do DESTINO (antes de processar)
    #    Precisa de token — vamos capturar abrindo o primeiro produto
    print("📋 ETAPA 2: Construindo mapa de informações adicionais do DESTINO...")
    print("-" * 70)

    # Abrir primeiro produto pra capturar token
    first_pid = to_process[0]["destino_id"]
    _page_transition_delay()
    first_json, first_token = _fetch_destino_product(page, first_pid)

    info_map = {}
    if first_token:
        info_map = _fetch_destino_info_map(page, first_token)
        if info_map:
            print(f"   ✅ {len(info_map)} infos adicionais mapeadas no DESTINO")
        else:
            print(f"   ⚠️ Nenhuma info adicional encontrada no DESTINO")
    else:
        print(f"   ⚠️ Token não capturado — infos adicionais não serão sincronizadas")

    # 4) Processar: abrir /edit → pegar JSON completo → PUT
    print(f"\n📦 ETAPA 3: Atualizando produtos...")
    print("-" * 70)

    updated = 0
    errors = 0
    update_log = []

    for idx, match in enumerate(to_process, 1):
        pid = match["destino_id"]
        name = match["destino_name"]
        origem_prod = match["origem_product"]

        print(f"\n[{idx}/{len(to_process)}] 🔄 {name[:60]} (ID {pid})")

        # Se é o primeiro, já temos o JSON
        if idx == 1 and first_json and pid == first_pid:
            destino_json = first_json
            auth_token = first_token
            print(f"   📄 (JSON já capturado)")
        else:
            # Pausa humana antes de navegar
            _page_transition_delay()
            print(f"   📄 Abrindo página de edição...")
            destino_json, auth_token = _fetch_destino_product(page, pid)

        if not destino_json:
            print(f"   ❌ Não conseguiu obter JSON do produto")
            errors += 1
            update_log.append({
                "id": pid, "nome": name, "status": 0,
                "resultado": "erro_json",
            })
            continue

        if not auth_token:
            # Tentar usar o token do primeiro produto
            auth_token = first_token
            if not auth_token:
                print(f"   ⚠️ Token não capturado — PUT pode falhar")

        # Montar payload (sem infos adicionais — essas vão por POST separado)
        payload = _build_put_payload(origem_prod, destino_json)

        # Pausa humana antes do PUT
        _human_delay(2.0, 4.0)

        # Enviar PUT
        print(f"   📤 Enviando atualização...")
        ok, status, body = _put_product(page, pid, payload, auth_token or "")

        if ok:
            updated += 1
            print(f"   ✅ Atualizado com sucesso (Status {status})")

            # Vincular infos adicionais (POST separado)
            origem_infos = origem_prod.get("informacoes_adicionais", [])
            if origem_infos and info_map:
                print(f"   📎 Vinculando {len(origem_infos)} infos adicionais...")
                ok_infos, fail_infos = _attach_additional_infos_to_product(
                    page, pid, origem_infos, info_map,
                )
                if ok_infos > 0:
                    print(f"   ✅ {ok_infos} infos adicionais vinculadas")
                if fail_infos > 0:
                    print(f"   ⚠️ {fail_infos} infos adicionais falharam")

            update_log.append({
                "id": pid, "nome": name, "status": status,
                "resultado": "sucesso",
            })
        else:
            errors += 1
            print(f"   ❌ Falha no PUT (Status {status})")
            print(f"      {body[:200]}")
            update_log.append({
                "id": pid, "nome": name, "status": status,
                "resultado": "falha", "detalhe": body[:300],
            })

        # Pausa stealth pós-PUT
        _post_put_delay()

    # 4) Resumo
    print("\n" + "=" * 70)
    print("✅ SYNC FINALIZADO!")
    print(f"   Produtos na ORIGEM:       {len(origem_products)}")
    print(f"   Matches encontrados:      {len(matches)}")
    print(f"   Processados (rate limit): {len(to_process)}")
    print(f"   Atualizados:              {updated}")
    print(f"   Erros:                    {errors}")

    if update_log:
        print(f"\n   📝 Detalhes:")
        for entry in update_log:
            icon = "✅" if entry["resultado"] == "sucesso" else "❌"
            print(f"      {icon} [{entry['id']}] {entry['nome'][:55]}")

        try:
            with open("produtos/sync_log.json", "w", encoding="utf-8") as f:
                json.dump(update_log, f, indent=2, ensure_ascii=False)
            print(f"\n   📋 Log salvo em: produtos/sync_log.json")
        except Exception:
            pass

    remaining = len(origem_products) - len(matches)
    if remaining > 0:
        print(f"\n   ⏳ {remaining} produtos da ORIGEM sem match no DESTINO.")

    if len(matches) > RATE_LIMIT:
        print(f"   ⏳ {len(matches) - RATE_LIMIT} matches restantes. Aumente RATE_LIMIT pra processar mais.")

    print("=" * 70)