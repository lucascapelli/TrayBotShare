import json
import logging
import os
import random
import time
from typing import Any, Optional

from service.sync_mod import destino_api
from service.sync_mod import destino_page
from service.sync_mod import domain
from service.sync_mod.services.additional_info_sync import sync_additional_infos
from service.sync_mod.services.variant_sync import sync_variants

logger = logging.getLogger("fix_produto")

ORIGEM_JSON_PATH = "produtos/ProdutosOrigem.json"
PRODUCT_ID = "47"


def _human_delay(min_s: float = 1.0, max_s: float = 2.5):
    mid = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mid, std)))
    time.sleep(delay)


def _short_delay():
    _human_delay(0.8, 1.8)


def _medium_delay():
    _human_delay(1.8, 3.0)


def _load_origem_reference(product_id: str) -> Optional[dict]:
    if not os.path.isfile(ORIGEM_JSON_PATH):
        logger.error("❌ Arquivo de referência não encontrado: %s", ORIGEM_JSON_PATH)
        return None

    try:
        with open(ORIGEM_JSON_PATH, "r", encoding="utf-8") as file:
            produtos = json.load(file)
    except Exception as exc:
        logger.error("❌ Falha ao ler referência de ORIGEM: %s", exc)
        return None
    pid = str(product_id)

    # Caso 1: formato antigo — lista de produtos
    if isinstance(produtos, list):
        for item in produtos:
            if str(item.get("produto_id", "")) == pid or str(item.get("id", "")) == pid:
                return item

    # Caso 2: formato single-product — dict direto
    elif isinstance(produtos, dict):
        # produto no nível raiz
        if str(produtos.get("produto_id", "")) == pid or str(produtos.get("id", "")) == pid:
            return produtos
        # produto dentro de chave 'data'
        if "data" in produtos and isinstance(produtos["data"], dict):
            inner = produtos["data"]
            if str(inner.get("produto_id", "")) == pid or str(inner.get("id", "")) == pid:
                return inner

    logger.error("❌ Produto de referência %s não encontrado em %s", pid, ORIGEM_JSON_PATH)
    return None


def run_fix_produto(context: Any, product_id: str = PRODUCT_ID) -> bool:
    print("\n" + "═" * 70)
    print(f"🛠️ REPAIR PRODUTO {product_id} (PUT + POST + POST)")
    print("   1) PUT dados simples")
    print("   2) POST infos adicionais")
    print("   3) POST variações")
    print("═" * 70)

    pages = context.pages
    if not pages:
        print("❌ Nenhuma página aberta no contexto.")
        return False
    page = pages[0]

    origem_ref = _load_origem_reference(product_id)
    if not origem_ref:
        return False

    destino_json, token = destino_page.fetch_product_and_token(page, str(product_id), logger=logger)
    if not destino_json:
        print(f"❌ Não conseguiu capturar JSON do DESTINO para o produto {product_id}")
        return False
    if not token:
        print("❌ Token do DESTINO não capturado")
        return False

    log_entry = {
        "destino_id": str(product_id),
        "destino_name": destino_json.get("name"),
        "origem_nome": origem_ref.get("nome"),
    }

    payload = domain.build_product_payload(origem_ref, destino_json)

    _medium_delay()
    ok_put, status_put, body_put = destino_api.put_product(page, str(product_id), payload, token)
    if ok_put:
        print(f"✅ PUT dados simples OK (status {status_put})")
    else:
        print(f"❌ PUT dados simples falhou (status {status_put})")
        if body_put:
            print(f"   detalhe: {body_put[:220]}")

    _medium_delay()
    sync_additional_infos(
        page,
        str(product_id),
        origem_ref.get("informacoes_adicionais", []),
        token,
        log_entry,
        short_delay=_short_delay,
        medium_delay=_medium_delay,
    )

    _medium_delay()
    sync_variants(
        page,
        str(product_id),
        origem_ref,
        token,
        log_entry,
        short_delay=_short_delay,
        medium_delay=_medium_delay,
        use_post_for_variants=True,
    )

    infos_status = (log_entry.get("infos_adicionais") or {}).get("status")
    vari_status = (log_entry.get("variacoes") or {}).get("status")

    print("\n" + "═" * 70)
    print(f"📌 RESUMO REPAIR [{product_id}]")
    print(f"   PUT: {'sucesso' if ok_put else 'falha'}")
    print(f"   Infos adicionais: {infos_status or 'executado'}")
    print(f"   Variações: {vari_status or 'executado'}")
    print("═" * 70)

    return ok_put
