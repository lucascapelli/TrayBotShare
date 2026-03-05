import logging
from typing import Any, Dict
from patchright.sync_api import Page

from service.sync_mod import destino_api
from service.sync_mod import domain
from service.sync_mod.services.additional_info_sync import sync_additional_infos

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
    use_post_for_variants: bool = False,
    infos_already_synced: bool = False,
    origin_base: str = "",
    cookies_origem=None,
):
    _log_section("SYNC VARIAÇÕES")

    if infos_already_synced:
        logger.warning("🧹 MODO INFOS ATIVADO — limpando todas as variações existentes...")
        variants_existentes, _ = destino_api.get_destino_variants(page, product_id, token, logger=logger)
        deleted_count = 0
        for v in variants_existentes:
            dv_id = str(v.get("id") or v.get("variant_id") or "")
            if dv_id:
                if destino_api.delete_variant(page, dv_id, token, logger=logger):
                    deleted_count += 1
                short_delay()
        logger.info("✅ %d variações deletadas (agora só usa Additional Infos)", deleted_count)
        log_entry["variants_deleted"] = deleted_count
        log_entry["variants_action"] = "deleted_all_due_to_infos_mode"
        return

    destino_product = destino_api.get_product_details(page, product_id, token, logger=logger)
    if not destino_product:
        logger.error("❌ Não conseguiu carregar detalhes do produto no DESTINO")
        return

    has_variation = str(destino_product.get("has_variation", ""))
    properties_len = len(destino_product.get("Properties") or [])
    additional_infos_len = len(destino_product.get("AdditionalInfos") or [])

    # Modo Additional Infos no destino → converter variações da origem
    if has_variation != "1" and properties_len == 0:
        logger.info("ℹ️ DESTINO em modo AdditionalInfos — convertendo variações da ORIGEM (produto %s)", product_id)

        # Tentar extrair opções precisas via API da origem
        origin_checked_options = None
        try:
            origin_checked_options = domain.extract_checked_options_from_variants(
                origem_product,
                page=page,
                origin_base=origin_base,
                cookies_origem=cookies_origem,
                logger=logger,
            )
            if origin_checked_options:
                logger.info("✅ Extraídas %d campos precisos via API origem: %s", len(origin_checked_options), list(origin_checked_options.keys()))
        except Exception as e:
            logger.warning("⚠️ Falha em extract_checked_options_from_variants: %s", str(e))

        if origin_checked_options:
            infos_merged = []
            for field_name, vals in origin_checked_options.items():
                opcoes = [{"nome": v.strip(), "valor": "0.00"} for v in vals if v.strip()]
                if opcoes:
                    infos_merged.append({"nome": field_name, "opcoes": opcoes})
            source_ctx = "api_checked_precise"
        else:
            logger.info("⚠️ Sem opções precisas via API — usando build_infos_for_additional_model como fallback")
            infos_merged = domain.build_infos_for_additional_model(origem_product)
            source_ctx = "variacoes_fallback"

        # Sempre tentar incluir infos textuais da origem ("Nome", "Frase", etc.)
        infos_text = domain._get_infos_from_product(origem_product)
        if infos_text:
            logger.info("🔗 Mesclando %d campos textuais da origem", len(infos_text))
            infos_merged = domain.merge_additional_infos(infos_text, infos_merged)

        if not infos_merged:
            logger.info("ℹ️ Nenhuma informação adicional gerada após merge — pulando sync")
            log_entry["variacoes"] = {"status": "sem_infos_geradas"}
            return

        try:
            sync_additional_infos(
                page,
                product_id,
                infos_merged,
                token,
                log_entry,
                short_delay=short_delay,
                medium_delay=medium_delay,
                create_missing_fields=True,
                source_context=source_ctx,
                origin_checked_options=origin_checked_options,
            )
            logger.info("✅ AdditionalInfos sincronizadas com sucesso (contexto: %s)", source_ctx)
            log_entry["variacoes"] = {
                "status": "convertido_para_additional_infos",
                "source_context": source_ctx,
                "campos_enviados": len(infos_merged),
            }
        except Exception as e:
            logger.error("❌ Erro ao sincronizar AdditionalInfos: %s", str(e))
            log_entry["variacoes"] = {"status": "falha_additional_infos", "erro": str(e)}
        return

    # Caso raro: destino ainda aceita variações via API (PUT/POST variants)
    # (provavelmente não vai entrar aqui nunca mais, mas mantemos por segurança)
    origem_variacoes = origem_product.get("variacoes", []) or []
    if not origem_variacoes:
        logger.info("ℹ️ ORIGEM sem variações — pulando sync de variants")
        log_entry["variacoes"] = {"status": "sem_variacoes_origem"}
        return

    # Comparação e sync via variants (se o destino permitir)
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
        sku_fixed = [{"type": domain.fix_opcao_banho_str(s.get("type", "").strip()), "value": s.get("value", "").strip()} for s in sku]
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

    # Restante do código de criar/deletar/enviar PUT/POST mantido
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
        if use_post_for_variants:
            logger.info("📤 POST variações: %d itens no payload", len(variants_payload))
        else:
            logger.info("📤 PUT variações: %d itens no payload", len(variants_payload))
        medium_delay()
        if use_post_for_variants:
            ok, status, body = destino_api.post_variants(page, product_id, variants_payload, token)
        else:
            ok, status, body = destino_api.put_variants(page, product_id, variants_payload, token)
        
        if ok:
            method = "POST" if use_post_for_variants else "PUT"
            logger.info("✅ Variações atualizadas via %s (status %d)", method, status)
        else:
            method = "POST" if use_post_for_variants else "PUT"
            logger.error("❌ %s variações falhou (status %d): %s", method, status, body[:200])
    else:
        logger.info("ℹ️ Nenhuma variação pra enviar via PUT")

    # Atualizado com variáveis dinâmicas conforme seu comentário no código
    log_entry["variacoes"] = {
        "origem_total": len(keys_origem),
        "destino_antes": len(keys_destino),
        "match": len(em_ambos),
        "criadas": len(so_origem),
        "deletadas": deleted,  
        "put_enviado": len(variants_payload),
        "metodo_envio": "POST" if use_post_for_variants else "PUT",
    }