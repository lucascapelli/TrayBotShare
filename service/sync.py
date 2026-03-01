# service/sync.py
import json
import time
from typing import Dict

def normalize_name(name: str) -> str:
    """Normaliza nome para comparação confiável"""
    if not name:
        return ""
    return " ".join(name.strip().split()).lower()


def products_by_name(storage) -> Dict[str, dict]:
    """Carrega JSON e indexa por nome normalizado"""
    data = storage.read_all()
    return {normalize_name(p.get("nome", "")): p for p in data if p.get("nome")}


def prepare_update_payload(origem: dict, destino: dict) -> dict:
    """
    Atualiza TUDO com dados da ORIGEM
    EXCETO:
      - produto_id (não pode enviar)
      - seo_preview.link (mantém o link do Atacado)
    """
    payload = {
        "name": origem.get("nome"),
        "price": origem.get("preco"),
        "description": origem.get("descricao"),
        "stock": origem.get("estoque"),
        "minimum_stock": origem.get("estoque_minimo"),
        "category_name": origem.get("categoria"),
        "reference": origem.get("referencia"),
        "weight": origem.get("peso"),
        "height": origem.get("altura"),
        "width": origem.get("largura"),
        "length": origem.get("comprimento"),
        "active": origem.get("ativo"),
        "visible": origem.get("visivel"),
        "minimum_stock_alert": origem.get("notificacao_estoque_baixo"),
        "included_items": origem.get("itens_inclusos", ""),
        "additional_message": origem.get("mensagem_adicional", ""),
        "warranty": origem.get("tempo_garantia"),
        "AdditionalInfos": origem.get("informacoes_adicionais", []),
    }

    # SEO: mantemos o link do DESTINO (Atacado)
    destino_link = destino.get("seo_preview", {}).get("link")
    if destino_link:
        payload["url"] = {"https": destino_link}

    # Imagem principal (da origem)
    if origem.get("imagem_url"):
        payload["ProductImage"] = [{"https": origem["imagem_url"]}]

    return payload


def update_product_in_destino(page, destino_id: str, payload: dict) -> bool:
    """Faz o PUT real no Atacado"""
    try:
        url = f"https://www.grasielyatacado.com.br/admin/products/{destino_id}"

        response = page.request.put(
            url=url,
            data=json.dumps({"data": payload}),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest"
            }
        )

        if response.ok:
            print(f"      ✅ SUCESSO → ID {destino_id}")
            return True
        else:
            print(f"      ❌ FALHA {response.status} → {response.text[:300]}")
            return False

    except Exception as e:
        print(f"      ❌ Erro de requisição: {e}")
        return False


# ========================= FUNÇÃO PRINCIPAL =========================
def run_sync(context, storage_origem, storage_destino, origem_url, user, password):
    print("\n" + "=" * 70)
    print("🔄 SYNC: ORIGEM → DESTINO (Atualizando Atacado com dados da Grasiely)")
    print("=" * 70)

    origem_dict = products_by_name(storage_origem)
    destino_dict = products_by_name(storage_destino)

    print(f"📊 Origem   → {len(origem_dict)} produtos")
    print(f"📊 Destino  → {len(destino_dict)} produtos")

    matches = []
    for nome_norm, prod_origem in origem_dict.items():
        if nome_norm in destino_dict:
            prod_destino = destino_dict[nome_norm]
            matches.append((prod_origem, prod_destino))
            print(f"   ✅ Match: {prod_origem.get('nome')[:70]}...")

    print(f"\n🔍 {len(matches)} produtos encontrados para sincronizar")

    if not matches:
        print("❌ Nenhum produto em comum.")
        return

    # Login NO DESTINO (Atacado) - usando o novo authenticate
    print("\n🔑 Logando no site do Atacado...")
    from .auth import authenticate   # import relativo

    # Cria uma nova página a partir do contexto
    page = context.new_page()
    page = authenticate(
        page,
        "https://www.grasielyatacado.com.br/admin/products/list",
        user,
        password
    )

    if not page:
        print("❌ Falha no login do Atacado")
        return

    # Atualiza um por um
    success = 0
    for idx, (prod_origem, prod_destino) in enumerate(matches, 1):
        destino_id = prod_destino.get("produto_id")
        nome = prod_origem.get("nome", "Sem nome")[:60]

        print(f"[{idx}/{len(matches)}] Atualizando → {nome} (ID destino: {destino_id})")

        payload = prepare_update_payload(prod_origem, prod_destino)

        if update_product_in_destino(page, destino_id, payload):
            success += 1

        time.sleep(1.3)  # delay seguro

    print("\n" + "=" * 60)
    print(f"🎉 SYNC CONCLUÍDO!")
    print(f"✅ {success}/{len(matches)} produtos atualizados no Atacado")
    print("=" * 60)