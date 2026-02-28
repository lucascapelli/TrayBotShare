# service/sync.py
import json
from service.auth import authenticate
from service.scraper import collect_all_products  # só pra reaproveitar se precisar

def products_by_name(storage) -> dict:
    """Carrega JSON e indexa por nome (chave única)"""
    data = storage.read_all()
    return {p["nome"].strip().lower(): p for p in data if p.get("nome")}

def run_sync(context, storage_origem, storage_destino, origem_url, user, password, cookie_files):
    origem_dict = products_by_name(storage_origem)
    destino_dict = products_by_name(storage_destino)

    comuns = set(origem_dict.keys()) & set(destino_dict.keys())
    print(f"✅ {len(comuns)} produtos com nome idêntico encontrados")

    to_update = []
    for nome in comuns:
        o = origem_dict[nome]
        d = destino_dict[nome]

        # Comparação ignorando APENAS o link de SEO
        seo_link_origem = o.get("seo_preview", {}).get("link", "")
        seo_link_destino = d.get("seo_preview", {}).get("link", "")

        # Remove temporariamente o SEO pra comparar o resto
        o_clean = {k: v for k, v in o.items() if k != "seo_preview"}
        d_clean = {k: v for k, v in d.items() if k != "seo_preview"}

        if o_clean != d_clean:
            to_update.append((o["produto_id"], d))  # (id_origem, dados_destino)

    if not to_update:
        print("🎉 Tudo já está sincronizado! Nada pra alterar.")
        return

    print(f"🔄 {len(to_update)} produtos precisam ser atualizados na ORIGEM...")

    # Login na origem (stealth igual antes)
    page = authenticate(context, origem_url, user, password, cookie_files)
    if not page:
        print("❌ Falha no login da origem")
        return

    # ==================== UPDATE REAL ====================
    for produto_id, dados_destino in to_update:
        try:
            # Pega o full_data do destino
            full_destino = dados_destino.get("full_data")
            if not full_destino:
                print(f"⚠️ Produto {produto_id} sem full_data (rode coleta novamente)")
                continue

            # Mantém o SEO link da ORIGEM (exceção que você pediu)
            if "url" in full_destino and isinstance(full_destino["url"], dict):
                full_destino["url"]["https"] = dados_destino.get("seo_preview", {}).get("link") or full_destino["url"].get("https")

            # AQUI VAI O CÓDIGO DE UPDATE (você só precisa descobrir 1x o endpoint)
            # Exemplo (substitua pelos dados reais que você vai me passar):
            response = page.request.put(
                f"https://www.grasiely.com.br/admin/products/{produto_id}",  # ← MUDE AQUI
                data=json.dumps({"data": full_destino}),                 # ← ou só full_destino
                headers={"Content-Type": "application/json", "Accept": "application/json"}
            )

            if response.ok:
                print(f"✅ Atualizado: {produto_id}")
            else:
                print(f"❌ Falha {produto_id} → {response.status} {response.text[:100]}")

        except Exception as e:
            print(f"❌ Erro ao atualizar {produto_id}: {e}")

    print("🎉 SYNC CONCLUÍDO!")