# service/fix_produto_47.py
# Script de recovery para o produto 47 que foi corrompido pelo sync anterior.
#
# O que aconteceu: o POST de infos adicionais provavelmente mandou
# "Opção Banho" (sem "do"), que trava o servidor da Tray.
# Agora o produto ficou num estado inconsistente.
#
# Estratégia de fix:
#   1. POST com selected_items VAZIO → limpa todos os vínculos (reseta)
#   2. Espera
#   3. Re-vincula as infos corretas (com "Opção do Banho")
#
# Uso: importar e chamar run_fix_produto(context, product_id)
#      ou rodar dentro do fluxo principal.

import json
import logging
import time
import random
from typing import Any, List, Optional

from patchright.sync_api import Page

logger = logging.getLogger("fix_produto")

DESTINO_BASE = "https://www.grasielyatacado.com.br"
PRODUCT_ID = "47"  # Produto corrompido


def _human_delay(min_s: float = 2.0, max_s: float = 5.0):
    mid = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mid, std)))
    time.sleep(delay)


# ─────────────────────────────────────────────────────────────────────────
# ETAPA 1: Limpar TODOS os vínculos de infos adicionais (POST vazio)
# ─────────────────────────────────────────────────────────────────────────
def _clear_additional_infos(page: Page, product_id: str) -> bool:
    """
    Manda POST com selected_items vazio pra LIMPAR todas as infos vinculadas.
    Isso deve resetar o estado corrompido.
    """
    # Primeiro navega pra página de infos (seta referrer + sessão)
    nav_url = (
        f"{DESTINO_BASE}/admin/#/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    print(f"   📂 Navegando pra página de infos adicionais...")
    try:
        page.goto(nav_url, wait_until="networkidle", timeout=20000)
        _human_delay(2.0, 4.0)
    except Exception as e:
        print(f"   ⚠️ Navegação falhou (tentando POST mesmo assim): {e}")

    # POST com corpo MÍNIMO — sem selected_items = limpa tudo
    # O servidor espera pelo menos _method e id_produto
    body_limpo = "&".join([
        "_method=POST",
        "_method=POST",
        f"id_produto={product_id}",
        "data%5BAdditionalProductInfo%5D%5Bherda_prazo%5D=0",
        "data%5BAdditionalProductInfo%5D%5Bprazo%5D=0",
    ])

    endpoint = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )

    print(f"   🧹 Enviando POST VAZIO pra limpar infos...")
    print(f"   📍 Endpoint: {endpoint}")

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
                        url: resp.url,
                        bodyLength: text.length,
                        snippet: text.substring(0, 500),
                    };
                } catch(e) {
                    return {status: 0, error: e.message};
                }
            }
        """, [endpoint, body_limpo])

        status = result.get("status", 0)
        ok = result.get("ok", False)
        redirected = result.get("redirected", False)
        error = result.get("error", "")

        print(f"   📨 Resultado: status={status}, ok={ok}, redirected={redirected}")

        if error:
            print(f"   ❌ Erro: {error}")
            return False

        if ok or status in (200, 302):
            print(f"   ✅ POST vazio aceito — infos devem estar limpas")
            return True
        else:
            print(f"   ❌ Status inesperado: {status}")
            print(f"   📄 Snippet: {result.get('snippet', '')[:200]}")
            return False

    except Exception as e:
        print(f"   ❌ Erro no fetch: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────
# ETAPA 2: Verificar se limpou (GET na página de infos)
# ─────────────────────────────────────────────────────────────────────────
def _check_infos_after_clear(page: Page, product_id: str) -> bool:
    """Recarrega a página de infos e verifica se está limpa."""
    nav_url = (
        f"{DESTINO_BASE}/admin/#/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    print(f"\n   🔍 Verificando estado após limpeza...")

    try:
        page.goto(nav_url, wait_until="networkidle", timeout=20000)
        _human_delay(2.0, 3.0)

        # Tenta ver se a página carregou sem travar
        # Se carregou, já é um bom sinal (antes travava com "Opção Banho")
        title = page.title()
        print(f"   📄 Página carregou (título: '{title[:50]}')")

        # Screenshot pra debug visual (se possível)
        try:
            page.screenshot(path="produtos/fix_47_after_clear.png")
            print(f"   📸 Screenshot salvo: produtos/fix_47_after_clear.png")
        except Exception:
            pass

        return True

    except Exception as e:
        print(f"   ⚠️ Página pode ainda estar com problema: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────
# ETAPA 3 (OPCIONAL): Re-vincular infos corretas
# ─────────────────────────────────────────────────────────────────────────
def _relink_infos(
    page: Page,
    product_id: str,
    info_ids: List[str],
) -> bool:
    """
    Re-vincula infos adicionais ao produto.
    SÓ chamar DEPOIS de confirmar que a limpeza funcionou.
    
    info_ids: lista de IDs de infos adicionais do DESTINO pra vincular.
    """
    if not info_ids:
        print(f"   ℹ️ Nenhuma info pra re-vincular")
        return True

    # Navegar antes
    nav_url = (
        f"{DESTINO_BASE}/admin/#/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )
    try:
        page.goto(nav_url, wait_until="networkidle", timeout=15000)
        _human_delay(1.5, 3.0)
    except Exception:
        pass

    # Montar payload com as infos corretas
    parts = ["_method=POST", "_method=POST"]
    for iid in info_ids:
        parts.append(f"selected_items%5B%5D={iid}")
    parts.append(f"id_produto={product_id}")
    parts.append("data%5BAdditionalProductInfo%5D%5Bherda_prazo%5D=0")
    parts.append("data%5BAdditionalProductInfo%5D%5Bprazo%5D=0")
    for iid in info_ids:
        parts.append(f"sort%5B%5D={iid}-")
    body = "&".join(parts)

    endpoint = (
        f"{DESTINO_BASE}/mvc/adm/additional_product_info/"
        f"additional_product_info/edit/{product_id}"
    )

    print(f"   📎 Re-vinculando {len(info_ids)} infos: {info_ids}")

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
                    return {
                        status: resp.status,
                        ok: resp.ok,
                        redirected: resp.redirected,
                    };
                } catch(e) { return {status: 0, error: e.message}; }
            }
        """, [endpoint, body])

        ok = result.get("ok") or result.get("status") in (200, 302)
        if ok:
            print(f"   ✅ Infos re-vinculadas com sucesso")
            return True
        else:
            print(f"   ❌ Re-vinculação falhou: {result}")
            return False

    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────
# Alternativa: tentar via API JSON (caso o endpoint PHP não responda)
# ─────────────────────────────────────────────────────────────────────────
def _try_fix_via_product_put(page: Page, product_id: str, token: str) -> bool:
    """
    Alternativa: faz PUT no produto com AdditionalInfos vazio,
    pra tentar resetar pelo lado da API JSON.
    """
    url = f"{DESTINO_BASE}/admin/api/products/{product_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["Authorization"] = token

    payload = {
        "data": {
            "AdditionalInfos": [],  # Limpar infos via API
        }
    }

    print(f"   🔧 Tentando PUT com AdditionalInfos vazio via API...")

    try:
        resp = page.request.put(
            url=url,
            data=json.dumps(payload),
            headers=headers,
        )
        body = ""
        try:
            body = resp.text()[:300]
        except Exception:
            pass

        print(f"   📨 Status: {resp.status}, ok: {resp.ok}")

        if resp.ok:
            print(f"   ✅ PUT aceito")
            return True
        else:
            print(f"   ❌ PUT falhou: {body[:200]}")
            return False
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────
# Capturar token
# ─────────────────────────────────────────────────────────────────────────
def _capture_token(page: Page, product_id: str) -> Optional[str]:
    """Navega /products/{id}/edit pra capturar token."""
    auth_token = None

    def _on_response(response):
        nonlocal auth_token
        if auth_token:
            return
        try:
            ct = response.headers.get("content-type", "")
            if "application/json" not in ct:
                return
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                tok = response.request.headers.get("authorization")
                if tok:
                    auth_token = tok
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
        while auth_token is None and waited < 10000:
            page.wait_for_timeout(300)
            waited += 300
    except Exception:
        pass
    finally:
        page.remove_listener("response", _on_response)

    # Fallback localStorage
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

    return auth_token


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def run_fix_produto(context: Any, product_id: str = PRODUCT_ID):
    """
    Fix para produto corrompido.
    
    Tenta 3 abordagens em sequência:
      1. POST vazio no endpoint PHP (limpa vínculos)
      2. PUT com AdditionalInfos vazio via API JSON
      3. Verifica se página de infos carrega sem travar
    """
    print("\n" + "═" * 70)
    print(f"🔧 FIX PRODUTO {product_id}")
    print(f"   Objetivo: limpar infos adicionais corrompidas")
    print("═" * 70)

    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta no contexto.")
        return False
    page = pages[0]

    # Capturar token primeiro
    print(f"\n🔑 Capturando token...")
    token = _capture_token(page, product_id)
    if token:
        print(f"   ✅ Token capturado")
    else:
        print(f"   ⚠️ Token não capturado (tentando sem ele)")

    # ── Abordagem 1: POST vazio (endpoint PHP) ──
    print(f"\n📌 ABORDAGEM 1: POST vazio no endpoint PHP")
    print("─" * 60)
    _human_delay(2.0, 3.0)
    cleared = _clear_additional_infos(page, product_id)

    _human_delay(3.0, 5.0)

    # ── Abordagem 2: PUT via API JSON ──
    if token:
        print(f"\n📌 ABORDAGEM 2: PUT com AdditionalInfos=[] via API")
        print("─" * 60)
        _human_delay(2.0, 3.0)
        _try_fix_via_product_put(page, product_id, token)

    _human_delay(3.0, 5.0)

    # ── Verificar ──
    print(f"\n📌 VERIFICAÇÃO")
    print("─" * 60)
    ok = _check_infos_after_clear(page, product_id)

    if ok:
        print(f"\n✅ Página de infos carregou — produto parece recuperado")
        print(f"   Agora você pode:")
        print(f"   1. Testar adicionar infos manualmente no painel")
        print(f"   2. Se funcionar, rodar o sync (que agora usa 'Opção do Banho')")
    else:
        print(f"\n⚠️ Página ainda pode estar com problema")
        print(f"   Pode ser necessário limpar pelo banco de dados da Tray")

    print("═" * 70)
    return ok