# domain.py
import logging
import unicodedata
from typing import Dict, List

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
    return [{**info, "nome": fix_opcao_banho_str(info.get("nome", ""))} for info in infos]


def normalize(name: str) -> str:
    if not name:
        return ""
    raw = " ".join(str(name).strip().split()).lower()
    nfkd = unicodedata.normalize("NFKD", raw)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def canonical_info_name(nome: str) -> str:
    if not nome:
        return ""
    nome = str(nome).strip()
    lower = nome.lower()
    
    if any(k in lower for k in [
        "tamanho do aro", "tamanho aro", "tam aro", "aro", 
        "medida aro", "tamanho do anel", "tamanho anel", "medida anel"
    ]):
        logger.info("🔄 CANONICAL ARO: '%s' → 'Tamanho do Aro'", nome)
        return "Tamanho do Aro"
    
    return fix_opcao_banho_str(nome)


def _extract_sku_items_from_variant(variacao: dict) -> list:
    if not isinstance(variacao, dict):
        return []
    
    for key in ("sku", "Sku"):
        items = variacao.get(key)
        if isinstance(items, list):
            result = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                tipo = (item.get("type") or item.get("tipo") or "").strip()
                valor = (item.get("value") or item.get("valor") or "").strip()
                if tipo and valor:
                    result.append({"type": tipo, "value": valor})
            if result:
                return result
    
    for key in ("PropertyValue", "VariantPropertyValue", "property_value"):
        items = variacao.get(key)
        if isinstance(items, list):
            result = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                tipo = (item.get("property_name") or item.get("type") or item.get("name") or "").strip()
                valor = (item.get("value") or item.get("name") or "").strip()
                if tipo and valor and tipo != valor:
                    result.append({"type": tipo, "value": valor})
            if result:
                return result
    
    return []


def build_additional_infos_from_variacoes(origem_variacoes: list) -> list:
    grouped = {}
    for variacao in origem_variacoes or []:
        sku_items = _extract_sku_items_from_variant(variacao)
        for sku_item in sku_items:
            info_nome = canonical_info_name(sku_item.get("type", "").strip())
            option_nome = sku_item.get("value", "").strip()
            if not info_nome or not option_nome:
                continue
            key = normalize(info_nome)
            grouped.setdefault(key, {"nome": info_nome, "opcoes": {}})
            op_key = normalize(option_nome)
            grouped[key]["opcoes"][op_key] = {"nome": option_nome, "valor": "0.00"}
    
    result = [{"nome": v["nome"], "opcoes": list(v["opcoes"].values())} for v in grouped.values()]
    if result:
        logger.info("🔄 build_additional_infos_from_variacoes → %d campos gerados: %s", len(result), [r["nome"] for r in result])
    else:
        logger.info("build_additional_infos_from_variacoes → nenhum campo gerado")
    return result


def merge_additional_infos(origem_infos: list, variacoes_infos: list) -> list:
    merged = {}
    for info in (origem_infos or []) + (variacoes_infos or []):
        nome = canonical_info_name(info.get("nome", "").strip())
        if not nome:
            continue
        key = normalize(nome)
        merged.setdefault(key, {"nome": nome, "opcoes": {}})
        for op in info.get("opcoes") or []:
            op_nome = (op.get("nome") or "").strip()
            if op_nome:
                op_key = normalize(op_nome)
                merged[key]["opcoes"][op_key] = op
    result = [{"nome": v["nome"], "opcoes": list(v["opcoes"].values())} for v in merged.values()]
    logger.info("merge_additional_infos → %d campos finais após merge", len(result))
    return result


def _get_infos_from_product(produto: dict) -> list:
    if not isinstance(produto, dict):
        logger.warning("Produto ORIGEM não é dict válido")
        return []

    # Desembrulhar 'data' se existir
    if "data" in produto and isinstance(produto["data"], dict):
        produto = produto["data"]
        logger.debug("Desembrulhado 'data' do produto origem")

    candidates = [
        produto.get("AdditionalInfos"),
        produto.get("additional_infos"),
        produto.get("informacoes_adicionais"),
    ]

    for infos in candidates:
        if isinstance(infos, list) and infos:
            logger.info("✅ Encontradas %d AdditionalInfos na origem", len(infos))
            converted = []
            for info in infos:
                nome = (info.get("name") or info.get("nome") or "").strip()
                if not nome:
                    continue
                opcoes = []
                opts = info.get("options") or info.get("opcoes") or info.get("PropertyValue") or info.get("values") or []
                for opt in opts:
                    if not isinstance(opt, dict):
                        continue
                    opt_nome = (opt.get("name") or opt.get("nome") or opt.get("value") or "").strip()
                    if opt_nome:
                        opt_val = opt.get("value") or opt.get("price") or "0.00"
                        opcoes.append({"nome": opt_nome, "valor": str(opt_val)})

                # Preserve type and do NOT force dummy options for textual fields.
                tipo_original = info.get("type", "text")
                # antigo comportamento: criar opção dummy para text/textarea — REMOVIDO

                converted.append({
                    "nome": nome,
                    "opcoes": opcoes,
                    "tipo": tipo_original
                })

            if converted:
                logger.info("Retornando %d infos convertidas", len(converted))
                return converted

    logger.info("Nenhuma AdditionalInfos textual encontrada na origem")
    return []


def _get_variacoes_from_product(produto: dict) -> list:
    variacoes = produto.get("variacoes") or produto.get("Variant") or []
    if isinstance(variacoes, list):
        logger.info("Encontradas %d variações na origem", len(variacoes))
        return variacoes
    logger.info("Nenhuma variação encontrada na origem")
    return []


def build_infos_for_additional_model(origem_product: dict) -> list:
    infos_origem = fix_opcao_banho_list(_get_infos_from_product(origem_product))
    variacoes = _get_variacoes_from_product(origem_product)
    infos_variacoes = build_additional_infos_from_variacoes(variacoes)

    # FORÇADO: priorizar variações se não houver infos textuais
    if not infos_origem and infos_variacoes:
        logger.info("FORÇADO: usando SOMENTE variações como AdditionalInfos (%d campos)", len(infos_variacoes))
        return infos_variacoes

    merged = merge_additional_infos(infos_origem, infos_variacoes)
    logger.info(
        "build_infos_for_additional_model → %d textuais + %d variações = %d merged",
        len(infos_origem), len(infos_variacoes), len(merged)
    )
    return merged


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


def extract_checked_options_from_variants(
    origem_product: dict,
    page=None,
    origin_base=None,
    cookies_origem=None,
    logger=None,
) -> Dict[str, List[str]]:
    if not logger:
        logger = logging.getLogger("sync")

    result: Dict[str, List[str]] = {}

    product_id = str(origem_product.get("id") or origem_product.get("produto_id") or "")
    if not product_id:
        logger.warning("ID do produto não encontrado para extrair variações")
        return result

    if page and origin_base and cookies_origem:
        try:
            try:
                from . import destino_api
            except Exception:
                import service.sync_mod.destino_api as destino_api
        except Exception:
            destino_api = None

        try:
            logger.info("🔄 Tentando endpoint /products-variants para produto %s", product_id)
            cookie_str = ""
            try:
                if destino_api:
                    cookie_str = destino_api._build_cookie_header(cookies_origem)
            except Exception:
                cookie_str = ""

            headers = {
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": f"{origin_base}/admin/products/{product_id}/edit",
            }
            if cookie_str:
                headers["Cookie"] = cookie_str

            try:
                if destino_api:
                    token = destino_api._extract_origin_token(page)
                    if token:
                        headers["Authorization"] = token
                        logger.info("🔑 Usando Authorization Bearer para /products-variants")
            except Exception:
                pass

            url = (
                f"{origin_base}/admin/api/products-variants"
                f"?filter[product_id]={product_id}&page[size]=100&sort=order"
            )
            try:
                resp = page.request.get(url, headers=headers, timeout=25000)
                if resp.status == 200:
                    data = resp.json()
                    variants = data.get("data", [])
                    for var in variants:
                        sku_items = _extract_sku_items_from_variant(var)
                        for item in sku_items:
                            field_raw = (item.get("type") or item.get("property_name") or "").strip()
                            value = str(item.get("value") or item.get("name") or "").strip()
                            if not field_raw or not value:
                                continue
                            field_name = canonical_info_name(field_raw)
                            norm_field = normalize(field_name)
                            result.setdefault(norm_field, [])
                            if value not in result[norm_field]:
                                result[norm_field].append(value)

                    if result:
                        logger.info("🔄 [CHECKED PRECISO] Extraídos %d campos via /products-variants", len(result))
                        return result
            except Exception as exc:
                logger.warning("Erro chamando /products-variants: %s", exc)
        except Exception:
            pass

    # Fallback individual
    variacoes = _get_variacoes_from_product(origem_product)
    if not variacoes:
        return result

    for var in variacoes:
        var_id = str(var.get("id") or "")
        sku_items = _extract_sku_items_from_variant(var)
        if not sku_items and var_id and page and origin_base and cookies_origem:
            try:
                if destino_api:
                    details = destino_api.fetch_origin_variant_details(
                        page=page, origin_base=origin_base, product_id=product_id,
                        variant_id=var_id, cookies_origem=cookies_origem, logger=logger
                    )
                    if details:
                        sku_items = _extract_sku_items_from_variant(details)
            except Exception:
                pass

        for item in sku_items:
            field_raw = (item.get("type") or "").strip()
            value = str(item.get("value") or "").strip()
            if not field_raw or not value:
                continue
            field_name = canonical_info_name(field_raw)
            norm_field = normalize(field_name)
            result.setdefault(norm_field, [])
            if value not in result[norm_field]:
                result[norm_field].append(value)

    return result