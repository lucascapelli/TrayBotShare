import logging
from patchright.sync_api import Page

from service.sync_mod import destino_api
from service.sync_mod import domain

logger = logging.getLogger("sync")


def _log_section(title: str):
    logger.info("")
    logger.info("─" * 60)
    logger.info("  %s", title)
    logger.info("─" * 60)


def sync_variants(
    page: Page,
    product_id: str,
    origem_product: dict,
    token: str,
    log_entry: dict,
    short_delay,
    medium_delay,
):
    _log_section("SYNC VARIAÇÕES")

    origem_variacoes = origem_product.get("variacoes", [])
    if not origem_variacoes:
        logger.info("ℹ️ Produto ORIGEM não tem variações — pulando")
        log_entry["variacoes"] = {"status": "sem_variacoes_origem"}
        return

    destino_variants, _ = destino_api.get_destino_variants(page, product_id, token, logger=logger)

    destino_by_key = {}
    for dv in destino_variants:
        sku = dv.get("Sku", [])
        key = domain.variant_sku_key(sku)
        if key:
            destino_by_key[key] = dv
        dv_id = dv.get("id") or dv.get("variant_id")
        logger.info("    DESTINO variação ID=%s  Sku=%s", dv_id, [(s.get("type"), s.get("value")) for s in sku])

    origem_by_key = {}
    for ov in origem_variacoes:
        sku = ov.get("sku", [])
        sku_fixed = []
        for item in sku:
            sku_fixed.append({
                "type": domain.fix_opcao_banho_str((item.get("type") or "").strip()),
                "value": (item.get("value") or "").strip(),
            })
        ov["_sku_fixed"] = sku_fixed
        key = domain.variant_sku_key(sku_fixed)
        if key:
            origem_by_key[key] = ov
        logger.info("    ORIGEM variação Sku=%s", [(s["type"], s["value"]) for s in sku_fixed])

    keys_origem = set(origem_by_key.keys())
    keys_destino = set(destino_by_key.keys())

    em_ambos = keys_origem & keys_destino
    so_origem = keys_origem - keys_destino
    so_destino = keys_destino - keys_origem

    logger.info("📊 Comparação variações:")
    logger.info("    ORIGEM:  %d variações", len(keys_origem))
    logger.info("    DESTINO: %d variações", len(keys_destino))
    logger.info("    Match:   %d", len(em_ambos))
    logger.info("    Criar:   %d (só na ORIGEM)", len(so_origem))
    logger.info("    Deletar: %d (só no DESTINO)", len(so_destino))

    if so_origem:
        logger.info("🏷️ Verificando/criando valores de propriedade necessários...")
        prop_map = destino_api.get_destino_properties(page, token, logger=logger)

        for key in so_origem:
            ov = origem_by_key[key]
            for sku_item in ov.get("_sku_fixed", []):
                prop_type = sku_item["type"]
                prop_value = sku_item["value"]
                prop_id = prop_map.get(domain.normalize(prop_type))

                if not prop_id:
                    logger.warning(
                        "    ⚠️ Propriedade '%s' não existe no DESTINO — não é possível criar variação automaticamente",
                        prop_type,
                    )
                    continue

                existing_values = destino_api.get_property_values(page, prop_id, token, logger=logger)
                if domain.normalize(prop_value) not in existing_values:
                    logger.info("    📝 Criando valor '%s' na propriedade '%s' (ID %s)", prop_value, prop_type, prop_id)
                    short_delay()
                    destino_api.append_property_value(page, prop_id, prop_value, token, logger=logger)
                else:
                    logger.info("    ✅ Valor '%s' já existe em '%s'", prop_value, prop_type)

    deleted = 0
    for key in so_destino:
        dv = destino_by_key[key]
        dv_id = str(dv.get("id") or dv.get("variant_id", ""))
        if dv_id:
            short_delay()
            if destino_api.delete_variant(page, dv_id, token, logger=logger):
                deleted += 1

    variants_payload = []
    for key in keys_origem:
        ov = origem_by_key[key]
        variant_data = {}

        if key in destino_by_key:
            dv = destino_by_key[key]
            dv_id = dv.get("id") or dv.get("variant_id")
            if dv_id:
                variant_data["id"] = str(dv_id)

        sku_fixed = ov.get("_sku_fixed", [])
        variant_data["Sku"] = [{"type": s["type"], "value": s["value"]} for s in sku_fixed]

        if ov.get("preco") is not None:
            try:
                variant_data["price"] = f"{float(ov['preco']):.2f}"
            except (ValueError, TypeError):
                pass
        if ov.get("estoque") is not None:
            variant_data["stock"] = ov["estoque"]
        if ov.get("referencia"):
            variant_data["reference"] = ov["referencia"]
        if ov.get("peso"):
            variant_data["weight"] = ov["peso"]
        if ov.get("imagem_url"):
            variant_data["VariantImage"] = [{"https": ov["imagem_url"]}]

        variants_payload.append(variant_data)

    if variants_payload:
        logger.info("📤 PUT variações: %d itens no payload", len(variants_payload))
        medium_delay()
        ok, status, body = destino_api.put_variants(page, product_id, variants_payload, token)
        if ok:
            logger.info("✅ Variações atualizadas (status %d)", status)
        else:
            logger.error("❌ PUT variações falhou (status %d): %s", status, body[:200])
    else:
        logger.info("ℹ️ Nenhuma variação pra enviar via PUT")

    log_entry["variacoes"] = {
        "origem_total": len(keys_origem),
        "destino_antes": len(keys_destino),
        "match": len(em_ambos),
        "criadas": len(so_origem),
        "deletadas": deleted,
        "put_enviado": len(variants_payload),
    }
