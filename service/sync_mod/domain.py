import logging
from typing import Dict

logger = logging.getLogger("sync")


def fix_opcao_banho_str(nome: str) -> str:
    if not nome:
        return nome

    normalized = " ".join(nome.strip().split()).lower()
    aliases = {
        "opção banho": "Opção do Banho",
        "opcao banho": "Opção do Banho",
        "escolher opção do banho": "Opção do Banho",
        "escolher opcao do banho": "Opção do Banho",
    }
    canonical = aliases.get(normalized)
    if canonical:
        logger.warning("⚠️ FIX BANHO: '%s' → '%s'", nome, canonical)
        return canonical
    return nome


def fix_opcao_banho_list(infos: list) -> list:
    if not infos:
        return infos
    fixed = []
    for info in infos:
        copia = dict(info)
        copia["nome"] = fix_opcao_banho_str(copia.get("nome", ""))
        fixed.append(copia)
    return fixed


def normalize(name: str) -> str:
    if not name:
        return ""
    return " ".join(name.strip().split()).lower()


def api_headers(auth_token: str, content_type: str = "application/json") -> dict:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": content_type,
        "X-Requested-With": "XMLHttpRequest",
    }
    if auth_token:
        headers["Authorization"] = auth_token
    return headers


def build_product_payload(origem: dict, destino_json: dict) -> dict:
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

    if "price" in payload:
        try:
            payload["price"] = f"{float(payload['price']):.2f}"
        except (ValueError, TypeError):
            pass

    for origem_key, destino_key in [
        ("ativo", "active"),
        ("visivel", "visible"),
        ("notificacao_estoque_baixo", "minimum_stock_alert"),
    ]:
        if origem_key in origem:
            payload[destino_key] = "1" if origem[origem_key] else "0"

    destino_url = destino_json.get("url", {})
    destino_link = destino_url.get("https") if isinstance(destino_url, dict) else None
    if destino_link:
        payload["url"] = {"https": destino_link}
        logger.info("🔗 SEO link preservado: %s", destino_link[:80])

    origem_seo = origem.get("seo_preview", {})
    metatags = []
    if origem_seo.get("title"):
        title = fix_opcao_banho_str(origem_seo["title"])
        metatags.append({"type": "title", "content": title})
    if origem_seo.get("description"):
        desc = fix_opcao_banho_str(origem_seo["description"])
        metatags.append({"type": "description", "content": desc})
    if metatags:
        payload["metatag"] = metatags

    img = origem.get("imagem_url")
    if img:
        payload["ProductImage"] = [{"https": img}]

    return payload


def variant_sku_key(sku_list: list) -> str:
    if not sku_list:
        return ""

    parts = []
    for sku_item in sorted(sku_list, key=lambda x: (x.get("type", ""), x.get("value", ""))):
        sku_type = normalize(sku_item.get("type", ""))
        sku_value = normalize(sku_item.get("value", ""))
        parts.append(f"{sku_type}={sku_value}")
    return "|".join(parts)


def map_name_to_id(items: list, name_keys: tuple[str, ...] = ("name",), id_key: str = "id") -> Dict[str, str]:
    mapped: Dict[str, str] = {}
    for item in items:
        name = ""
        for key in name_keys:
            candidate = (item.get(key) or "").strip()
            if candidate:
                name = candidate
                break
        item_id = item.get(id_key)
        if name and item_id:
            mapped[normalize(name)] = str(item_id)
    return mapped
