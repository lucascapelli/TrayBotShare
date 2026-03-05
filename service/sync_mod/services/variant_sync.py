#variant_sync.py
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


def _collect_field_options_by_playwright(page: Page, field_name: str, short_delay, logger) -> list:
    """
    Tenta, via Playwright, clicar no campo dentro de `#product-variations-form`
    e retornar uma lista de textos visíveis que podem representar os valores.
    """
    results = []
    try:
        container = page.locator("#product-variations-form")
    except Exception:
        container = None

    # Tentar clicar no rótulo/field para expor os valores
    clicked = False
    attempts = []
    # Preparar versão normalizada do field_name
    try:
        fn_norm = domain.normalize(field_name)
    except Exception:
        fn_norm = (field_name or "").strip().lower()

    # Logar textos disponíveis no container para diagnóstico rápido
    try:
        if container is not None:
            sample_nodes = container.locator("label, h3, h4, .variation-label, .field-label, legend, .title, .form-group")
            sample_texts = []
            try:
                sample_texts = sample_nodes.all_text_contents()
            except Exception:
                sample_texts = []
            # reduzir e normalizar
            seen_sample = []
            for t in sample_texts:
                s = (t or "").strip()
                if not s:
                    continue
                n = domain.normalize(s)
                if n not in seen_sample:
                    seen_sample.append(n)
            logger.info("🔍 Textos detectados no container (amostra): %s", seen_sample[:20])
    except Exception:
        pass
    # Try several strategies to click/open the field
    try:
        # 1) tentar achar por igualdade normalizada dentro do container (mais robusto)
        if container is not None and not clicked:
            try:
                # coletar textos no container para comparação
                texts = []
                try:
                    texts = container.locator("label, h3, h4, .variation-label, .field-label, legend, .title, .form-group").all_text_contents()
                except Exception:
                    texts = []
                for t in texts:
                    s = (t or "").strip()
                    if not s:
                        continue
                    if domain.normalize(s) == fn_norm or fn_norm in domain.normalize(s) or domain.normalize(s) in fn_norm:
                        try:
                            # clicar pelo texto original (exact)
                            try:
                                loc = container.get_by_text(s, exact=True)
                                if loc.count() > 0:
                                    loc.first.click()
                                    clicked = True
                                    attempts.append("container_match_text_exact")
                                    break
                            except Exception:
                                pass
                            try:
                                loc = container.get_by_text(s)
                                if loc.count() > 0:
                                    loc.first.click()
                                    clicked = True
                                    attempts.append("container_match_text_partial")
                                    break
                            except Exception:
                                pass
                        except Exception:
                            continue
            except Exception:
                pass

        # 2) partial match inside container
        if not clicked and container is not None:
            try:
                loc = container.get_by_text(field_name)
                if loc.count() > 0:
                    loc.first.click()
                    clicked = True
                    attempts.append("container_partial")
            except Exception:
                pass

        # 3) exact global
        if not clicked:
            try:
                loc = page.get_by_text(field_name, exact=True)
                if loc.count() > 0:
                    loc.first.click()
                    clicked = True
                    attempts.append("page_exact")
            except Exception:
                pass

        # 4) partial global
        if not clicked:
            try:
                loc = page.get_by_text(field_name)
                if loc.count() > 0:
                    loc.first.click()
                    clicked = True
                    attempts.append("page_partial")
            except Exception:
                pass

        # 5) XPath label contains text (case/whitespace tolerant)
        if not clicked:
            try:
                xpath = f"//label[contains(normalize-space(string(.)), '{field_name}')]|//button[contains(normalize-space(string(.)), '{field_name}')]"
                loc = page.locator(f"xpath={xpath}")
                if loc.count() > 0:
                    loc.first.click()
                    clicked = True
                    attempts.append("xpath_label")
            except Exception:
                pass

        # 6) try to click a nearby button inside container (caret/open)
        if not clicked and container is not None:
            try:
                possible = container.locator("button, .dropdown-toggle, .select2-selection, .caret, .toggle")
                if possible.count() > 0:
                    possible.first.click()
                    clicked = True
                    attempts.append("container_fallback_button")
            except Exception:
                pass
    except Exception:
        logger.debug("Erro nas tentativas de abrir campo DOM para '%s'", field_name)

    logger.info("🔎 Tentativas de abrir campo '%s': %s -> clicked=%s", field_name, attempts, clicked)

    short_delay()

    # Colete textos de vários seletores relativos ao container (ou globalmente)
    seen = set()
    selectors = ["[role='option']", ".dropdown-menu li", "li", "button", "label", "a", "span", "div"]
    for sel in selectors:
        try:
            if container is not None:
                nodes = container.locator(sel)
            else:
                nodes = page.locator(sel)
            try:
                texts = nodes.all_text_contents()
            except Exception:
                texts = []
            for t in texts:
                s = (t or "").strip()
                if not s:
                    continue
                if s.lower() == field_name.lower():
                    continue
                if s not in seen:
                    seen.add(s)
                    results.append(s)
        except Exception:
            continue

    # Também tentar números/valores visíveis diretamente por texto exato (útil para 12,13,14...)
    # Procurar até 200 elementos de texto curto
    try:
        texts = page.locator("#product-variations-form").all_text_contents()
        for t in texts:
            s = (t or "").strip()
            if not s:
                continue
            if s.lower() == field_name.lower():
                continue
            if len(s) <= 30 and s not in seen:
                seen.add(s)
                results.append(s)
    except Exception:
        pass

    logger.info("🔎 Coletadas %d opções para '%s': %s", len(results), field_name, results[:20])
    return results


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

    # Se o fluxo informou que as infos já foram sincronizadas a partir das variações,
    # deletamos TODAS as variações existentes e não criamos/atualizamos nenhuma.
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
    if destino_product:
        has_variation = str(destino_product.get("has_variation", ""))
        properties_len = len(destino_product.get("Properties") or [])
        additional_infos_len = len(destino_product.get("AdditionalInfos") or [])

        # Regra genérica:
        # alguns produtos trabalham com variações via API (has_variation/properties/variant),
        # enquanto outros usam apenas informações adicionais (AdditionalInfos).
        # Nesses casos de infos adicionais, o endpoint de variações tende a falhar e deve ser pulado.
        if has_variation != "1" and properties_len == 0 and additional_infos_len > 0:
            logger.info("ℹ️ Destino em modo AdditionalInfos — usando contorno para variações da origem (produto %s)", product_id)

            # origem_variacoes primariamente vem de origem_product['variacoes'],
            # mas também aceitamos Variant IDs como fallback.
            origem_variacoes = origem_product.get("variacoes", []) or []
            variant_list = origem_product.get("Variant") or []
            variant_ids = []
            for v in variant_list:
                if isinstance(v, dict) and v.get("id"):
                    variant_ids.append(str(v.get("id")))
                elif isinstance(v, str) and v.isdigit():
                    variant_ids.append(v)

            if origem_variacoes or variant_ids:
                count_src = len(origem_variacoes) if origem_variacoes else len(variant_ids)
                logger.info("🔁 Convertendo %d variações da origem para AdditionalInfos", count_src)

                # PRIORIDADE: tentar obter checked options precisas a partir das variações (enriquecimento)
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
                        logger.info("✅ Usando VARIAÇÕES da origem como fonte PRECISA de checkboxes (%d campos)", len(origin_checked_options))
                except Exception as e:
                    logger.warning("⚠️ Falha extraindo checked options das variações: %s", e)

                # Se não obteve valores precisos, tentar mapear via Variant IDs API
                if not origin_checked_options and variant_ids and origin_base:
                    try:
                        logger.info("🔎 Tentando mapear Variant IDs via API (fallback) — %d ids", len(variant_ids))
                        prop_map = destino_api.map_origin_variant_ids_to_properties(
                            page, origin_base, variant_ids, cookies_origem, logger
                        )
                        if prop_map:
                            origin_checked_options = prop_map
                            log_entry["variants_options_collected"] = prop_map
                            logger.info("✅ Mapeamento Variant IDs → propriedades obtido (%d campos)", len(prop_map))
                    except Exception as e:
                        logger.warning("⚠️ Erro mapeando Variant IDs: %s", e)

                # Montar infos_merged a partir do dict preciso ou como fallback gerar via SKU
                infos_merged = None
                source_ctx = "variacoes_fallback"
                if origin_checked_options:
                    infos_merged = []
                    for field_name, vals in origin_checked_options.items():
                        opcoes = [{"nome": v, "valor": "0.00"} for v in vals]
                        infos_merged.append({"nome": field_name, "opcoes": opcoes})
                    source_ctx = "variacoes_checked_precise"
                else:
                    infos_merged = domain.build_infos_for_additional_model(origem_product)
                    source_ctx = "variacoes_fallback"

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
                    logger.info("✅ AdditionalInfos sincronizadas a partir das variações (produto %s)", product_id)
                except Exception as e:
                    logger.error("❌ Falha ao sincronizar AdditionalInfos a partir das variações: %s", e)

                log_entry["variacoes"] = {
                    "status": "convertido_para_additional_infos",
                    "origem_variacoes": len(origem_variacoes) if origem_variacoes else len(variant_ids),
                    "source_context": source_ctx,
                }
            else:
                logger.info("ℹ️ Sem variações na origem — pulando mesmo no modo infos")
                log_entry["variacoes"] = {"status": "sem_variacoes_origem"}

            return

    origem_variacoes = origem_product.get("variacoes", [])
    if not origem_variacoes:
        # Tentar mapear a partir de Variant IDs (quando ORIGEM só fornece ids)
        variant_list = origem_product.get("Variant") or []
        variant_ids = []
        for v in variant_list:
            if isinstance(v, dict) and v.get("id"):
                variant_ids.append(str(v.get("id")))
            elif isinstance(v, str) and v.isdigit():
                variant_ids.append(v)

        if variant_ids and origin_base:
            try:
                logger.info("🔎 ORIGEM contém Variant IDs (%d) — mapeando via API...", len(variant_ids))
                prop_map = destino_api.map_origin_variant_ids_to_properties(
                    page, origin_base, variant_ids, cookies_origem, logger
                )
                if prop_map:
                    # transformar em estrutura de AdditionalInfos
                    infos_from_variants = []
                    for prop_name, vals in prop_map.items():
                        op_list = [{"nome": v, "valor": "0.00"} for v in vals]
                        infos_from_variants.append({"nome": prop_name, "opcoes": op_list})
                    log_entry["variants_options_collected"] = prop_map
                    # Chamar sync_additional_infos para aplicar no DESTINO
                    try:
                        sync_additional_infos(
                            page,
                            product_id,
                            infos_from_variants,
                            token,
                            log_entry,
                            short_delay=short_delay,
                            medium_delay=medium_delay,
                            create_missing_fields=True,
                            source_context="variant_ids_api",
                            origin_checked_options=None,
                        )
                        logger.info("✅ AdditionalInfos sincronizadas a partir de Variant IDs (API)")
                        log_entry["variants_dom_synced"] = True
                    except Exception as e:
                        logger.warning("Falha sync_additional_infos a partir de Variant IDs: %s", e)
                        log_entry["variants_dom_synced"] = False
                else:
                    logger.info("⚠️ Nenhum mapeamento encontrado para Variant IDs")
            except Exception as e:
                logger.warning("Erro mapeando Variant IDs: %s", e)

        logger.info("ℹ️ Produto ORIGEM não tem variações — pulando")
        log_entry["variacoes"] = {"status": "sem_variacoes_origem"}
        return

    # Nova abordagem: coletar opções visíveis no editor do produto via seletores Playwright
    # para tentar mapear os tipos de variação (ex: "Tamanho do Aro") aos valores (ex: 12,13,14)
    field_options_map = {}
    try:
        for ov in origem_variacoes:
            # extrair sku items via helper (suporta vários formatos)
            sku_items = domain._extract_sku_items_from_variant(ov) or ov.get("sku", [])
            for si in sku_items:
                prop_type = domain.fix_opcao_banho_str((si.get("type") or "").strip())
                if not prop_type:
                    continue
                if prop_type in field_options_map:
                    continue
                try:
                    opts = _collect_field_options_by_playwright(page, prop_type, short_delay)
                    if opts:
                        logger.info("🔎 Coletado %d opções para campo '%s'", len(opts), prop_type)
                        field_options_map[prop_type] = opts
                    else:
                        logger.info("🔎 Nenhuma opção encontrada via DOM para '%s'", prop_type)
                except Exception as e:
                    logger.warning("Erro coletando opções DOM para '%s': %s", prop_type, e)
    except Exception:
        pass

    if field_options_map:
        log_entry["variants_options_collected"] = field_options_map
        # Construir estrutura AdditionalInfos a partir das opções coletadas no DOM
        infos_from_dom = []
        for prop_type, opts in field_options_map.items():
            op_list = []
            for o in opts:
                if not o or not str(o).strip():
                    continue
                op_list.append({"nome": str(o).strip(), "valor": "0.00"})
            if op_list:
                infos_from_dom.append({"nome": prop_type, "opcoes": op_list})

        if infos_from_dom:
            logger.info("🔗 Mapeando %d campos DOM → AdditionalInfos: %s", len(infos_from_dom), [i["nome"] for i in infos_from_dom])
            # Registrar antes de tentar sincronizar
            log_entry["variants_dom_fields"] = [i["nome"] for i in infos_from_dom]
            try:
                sync_additional_infos(
                    page,
                    product_id,
                    infos_from_dom,
                    token,
                    log_entry,
                    short_delay=short_delay,
                    medium_delay=medium_delay,
                    create_missing_fields=True,
                    source_context="dom_variations",
                    origin_checked_options=None,
                )
                logger.info("✅ AdditionalInfos sincronizadas a partir do DOM para %s", product_id)
                log_entry["variants_dom_synced"] = True
            except Exception as e:
                logger.error("❌ Falha ao sincronizar AdditionalInfos via DOM para %s: %s", product_id, e)
                log_entry["variants_dom_synced"] = False
                log_entry["variants_dom_error"] = str(e)

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

    log_entry["variacoes"] = {
        "origem_total": len(keys_origem),
        "destino_antes": len(keys_destino),
        "match": len(em_ambos),
        "criadas": len(so_origem),
        "deletadas": deleted,
        "put_enviado": len(variants_payload),
        "metodo_envio": "POST" if use_post_for_variants else "PUT",
    }
