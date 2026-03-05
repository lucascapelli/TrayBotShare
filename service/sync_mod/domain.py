#domain.py
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
    fixed = []
    for info in infos:
        copia = dict(info)
        copia["nome"] = fix_opcao_banho_str(copia.get("nome", ""))
        fixed.append(copia)
    return fixed

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
    
    # Regra forte para anéis
    if any(k in lower for k in [
        "tamanho do aro", "tamanho aro", "tam aro", "aro", 
        "medida aro", "tamanho do anel", "tamanho anel", "medida anel"
    ]):
        logger.info("🔄 CANONICAL ARO: '%s' → 'Tamanho do Aro'", nome)
        return "Tamanho do Aro"
    
    # Mantém fix do banho
    return fix_opcao_banho_str(nome)

# ══════════════════════════════════════════════════════════════════════
# EXTRAIR SKU ITEMS de uma variação (aceita múltiplos formatos Tray)
# ══════════════════════════════════════════════════════════════════════
def _extract_sku_items_from_variant(variacao: dict) -> list:
    if not isinstance(variacao, dict):
        return []
    # Formato 1/2: "sku" ou "Sku" com type/value
    for key in ("sku", "Sku"):
        items = variacao.get(key)
        if isinstance(items, list) and items:
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
    # Formato 3/4: "PropertyValue" ou "VariantPropertyValue"
    for key in ("PropertyValue", "VariantPropertyValue", "property_value"):
        items = variacao.get(key)
        if isinstance(items, list) and items:
            result = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                tipo = (
                    item.get("property_name")
                    or item.get("type")
                    or item.get("name")
                    or ""
                ).strip()
                valor = (
                    item.get("value")
                    or item.get("name")
                    or ""
                ).strip()
                if tipo and valor and tipo != valor:
                    result.append({"type": tipo, "value": valor})
                elif tipo and not valor:
                    val2 = (item.get("name") or "").strip()
                    if val2 and val2 != tipo:
                        result.append({"type": tipo, "value": val2})
            if result:
                return result
    return []

def build_additional_infos_from_variacoes(origem_variacoes: list) -> list:
    grouped = {}
    for variacao in origem_variacoes or []:
        if not isinstance(variacao, dict):
            continue
        sku_items = _extract_sku_items_from_variant(variacao)
        for sku_item in sku_items:
            info_nome = canonical_info_name((sku_item.get("type") or "").strip())
            option_nome = (sku_item.get("value") or "").strip()
            if not info_nome or not option_nome:
                continue
            info_key = normalize(info_nome)
            if info_key not in grouped:
                grouped[info_key] = {
                    "nome": info_nome,
                    "opcoes": {},
                }
            option_key = normalize(option_nome)
            grouped[info_key]["opcoes"][option_key] = {
                "nome": option_nome,
                "valor": "0.00",
            }
    result = []
    for item in grouped.values():
        result.append(
            {
                "nome": item["nome"],
                "opcoes": list(item["opcoes"].values()),
            }
        )
    if result:
        logger.info(
            "🔄 Variações → %d campos de info adicional: %s",
            len(result),
            [(r["nome"], len(r["opcoes"])) for r in result],
        )
    return result

def merge_additional_infos(origem_infos: list, infos_from_variacoes: list) -> list:
    merged = {}
    for info in (origem_infos or []) + (infos_from_variacoes or []):
        nome = canonical_info_name((info.get("nome") or "").strip())
        if not nome:
            continue
        key = normalize(nome)
        if key not in merged:
            merged[key] = {"nome": nome, "opcoes": {}}
        for op in info.get("opcoes") or []:
            op_nome = (op.get("nome") or "").strip()
            if not op_nome:
                continue
            op_key = normalize(op_nome)
            merged[key]["opcoes"][op_key] = {
                "nome": op_nome,
                "valor": op.get("valor", "0.00"),
            }
    result = []
    for item in merged.values():
        result.append({"nome": item["nome"], "opcoes": list(item["opcoes"].values())})
    return result

def build_infos_for_additional_model(origem_product: dict) -> list:
    infos_origem = fix_opcao_banho_list(
        _get_infos_from_product(origem_product)
    )
    variacoes = _get_variacoes_from_product(origem_product)
    infos_variacoes = build_additional_infos_from_variacoes(variacoes)
    merged = merge_additional_infos(infos_origem, infos_variacoes)
    logger.info(
        "📦 build_infos_for_additional_model: %d infos origem + %d de variações = %d merged",
        len(infos_origem), len(infos_variacoes), len(merged),
    )
    return merged

def _get_infos_from_product(produto: dict) -> list:
    infos = produto.get("informacoes_adicionais") or []
    if infos and isinstance(infos, list):
        first = infos[0] if infos else {}
        if isinstance(first, dict) and (first.get("nome") or first.get("opcoes")):
            return infos
    tray = produto.get("AdditionalInfos") or []
    if tray and isinstance(tray, list):
        converted = []
        for info in tray:
            if not isinstance(info, dict):
                continue
            nome = (info.get("name") or "").strip()
            if not nome:
                continue
            opcoes = []
            for opt in (info.get("options") or []):
                if isinstance(opt, dict):
                    opt_nome = (opt.get("name") or "").strip()
                    if opt_nome:
                        opcoes.append({"nome": opt_nome, "valor": str(opt.get("value") or "0.00")})
            converted.append({"nome": nome, "opcoes": opcoes})
        if converted:
            return converted
    if infos and isinstance(infos, list):
        first = infos[0] if infos else {}
        if isinstance(first, dict) and first.get("name"):
            converted = []
            for info in infos:
                if not isinstance(info, dict):
                    continue
                nome = (info.get("name") or "").strip()
                if nome:
                    opcoes = []
                    for opt in (info.get("options") or []):
                        if isinstance(opt, dict):
                            opt_nome = (opt.get("name") or "").strip()
                            if opt_nome:
                                opcoes.append({"nome": opt_nome, "valor": "0.00"})
                    converted.append({"nome": nome, "opcoes": opcoes})
            return converted
    return []

def _get_variacoes_from_product(produto: dict) -> list:
    variacoes = produto.get("variacoes") or []
    if variacoes and isinstance(variacoes, list):
        for v in variacoes:
            if isinstance(v, dict) and _extract_sku_items_from_variant(v):
                return variacoes
    variants = produto.get("Variant") or []
    if variants and isinstance(variants, list):
        logger.info("🔄 Convertendo %d Variant (Tray) para formato interno", len(variants))
        return variants
    return variacoes if isinstance(variacoes, list) else []

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
    """
    Extrai opções checked (valores selecionados) das variações da origem.
    Prioriza o endpoint /products-variants?filter[product_id]=... que traz Sku com type/value.
    """
    if not logger:
        logger = logging.getLogger("sync")

    result: Dict[str, List[str]] = {}

    product_id = str(origem_product.get("id") or origem_product.get("produto_id") or "")
    if not product_id:
        logger.warning("ID do produto não encontrado para extrair variações")
        return result

    # === PRIORIDADE 1: Endpoint de listagem completa de variants por product_id ===
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

            # Tenta adicionar token se disponível (extraído da página)
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
                    logger.info("✅ /products-variants retornou %d variações completas", len(variants))

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
                        logger.info("🔄 [CHECKED PRECISO] Extraídos %d campos via /products-variants: %s", len(result), result)
                        return result
                    else:
                        logger.warning("/products-variants retornou, mas sem valores extraíveis")
                else:
                    logger.warning("/products-variants status %d", resp.status)
            except Exception as exc:
                logger.warning("Erro chamando /products-variants: %s", exc)

        except Exception:
            pass

    # === FALLBACK: se não pegou via listagem, tenta enriquecer por ID individual (já existe) ===
    variacoes = _get_variacoes_from_product(origem_product)
    if not variacoes:
        logger.warning("Nenhuma variação para extrair (nem via listagem nem individual)")
        return result

    enriched = False
    for var in variacoes:
        var_id = str(var.get("id") or "")
        sku_items = _extract_sku_items_from_variant(var)

        if not sku_items and var_id and page and origin_base and cookies_origem:
            logger.info("🔄 Enriquecendo variant ID %s individualmente", var_id)
            try:
                try:
                    from . import destino_api
                except Exception:
                    import service.sync_mod.destino_api as destino_api
                details = destino_api.fetch_origin_variant_details(
                    page=page,
                    origin_base=origin_base,
                    product_id=product_id,
                    variant_id=var_id,
                    cookies_origem=cookies_origem,
                    logger=logger
                )
                if details:
                    sku_items = _extract_sku_items_from_variant(details)
                    enriched = True
            except Exception:
                logger.warning("Erro enriquecendo variant %s individualmente", var_id)

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

    if enriched:
        logger.info("✅ %d variações enriquecidas individualmente", len(variacoes))

    if result:
        logger.info("🔄 [CHECKED] %d campos: %s", len(result), result)
    else:
        logger.warning("⚠️ [CHECKED] Nenhum valor extraído (tentou listagem + individual)")

    return result