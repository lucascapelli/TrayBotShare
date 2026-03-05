import logging
import time
from typing import Dict, List, Optional

from patchright.sync_api import Page

from service.sync_mod import destino_api
from service.sync_mod import domain

logger = logging.getLogger("sync")


def _log_section(title: str):
    logger.info("")
    logger.info("─" * 60)
    logger.info("  %s", title)
    logger.info("─" * 60)


def sync_additional_infos(
    page: Page,
    product_id: str,
    origem_infos: list,
    token: str,
    log_entry: dict,
    short_delay,
    medium_delay,
    create_missing_fields: bool = False,
    source_context: str = "origem_infos",
    # ── Dados de opções checked da ORIGEM ──
    origin_checked_options: Optional[Dict[str, List[str]]] = None,
):
    """
    Sincroniza informações adicionais de um produto.

    FLUXO:
    1. Determina quais CAMPOS vincular (selected_items[])
    2. Determina quais OPÇÕES marcar (option_info[])
       → Se origin_checked_options disponível: usa APENAS as marcadas na ORIGEM
       → Senão: usa todas do JSON (fallback)
    3. Envia POST único com campos + opções corretos
    """
    _log_section("SYNC INFORMAÇÕES ADICIONAIS")

    if not origem_infos:
        logger.info("ℹ️ Produto ORIGEM não tem informações adicionais — pulando")
        log_entry["infos_adicionais"] = {"status": "sem_infos_origem"}
        return

    origem_infos = domain.fix_opcao_banho_list(origem_infos)

    info_catalog = destino_api.fetch_all_additional_infos_catalog(page, token, logger=logger)
    if not info_catalog:
        if not create_missing_fields:
            logger.warning("⚠️ Catálogo de infos do DESTINO vazio — impossível sincronizar")
            log_entry["infos_adicionais"] = {"status": "catalogo_vazio"}
            return
        logger.warning("⚠️ Catálogo vazio + create_missing_fields=True → tentando criar...")

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 1: Preparar campos + opções
    # ══════════════════════════════════════════════════════════════════════
    ids_desejados = []
    sort_entries = []
    option_info_entries = []  # formato: "OPTION_ID-FIELD_ID"
    nao_encontrados = []
    opcoes_nao_encontradas = {}
    opcoes_stats = {}  # para log

    for info in origem_infos:
        nome = domain.fix_opcao_banho_str((info.get("nome") or "").strip())
        if not nome:
            continue

        norm_nome = domain.normalize(nome)
        destino_info = info_catalog.get(norm_nome)
        destino_id = destino_info.get("id") if isinstance(destino_info, dict) else None

        opcoes_origem = info.get("opcoes") or []
        opcoes_nomes = [
            (op.get("nome") or "").strip()
            for op in opcoes_origem
            if (op.get("nome") or "").strip()
        ]

        # Criar campo/opções faltantes
        if create_missing_fields and ((not destino_info) or (opcoes_nomes and isinstance(destino_info, dict))):
            logger.info("    🔧 Garantindo campo '%s' com %d opções...", nome, len(opcoes_nomes))
            ensured = destino_api.ensure_additional_info_with_options(
                page, token, nome, opcoes_nomes, logger=logger,
            )
            if ensured:
                destino_info = ensured
                destino_id = ensured.get("id")
                info_catalog[norm_nome] = ensured

        if not destino_id:
            nao_encontrados.append(nome)
            logger.warning("    ❌ '%s' NÃO existe no catálogo DESTINO", nome)
            continue

        ids_desejados.append(destino_id)
        sort_entries.append(f"{destino_id}-")
        logger.info("    ✅ '%s' → DESTINO ID %s", nome, destino_id)

        # ══════════════════════════════════════════════════════════════
        # CRITICAL FIX: Determinar quais OPÇÕES marcar
        #
        # Se temos origin_checked_options → usar APENAS as marcadas
        # Senão → fallback: usar todas do JSON (pode marcar demais)
        # ══════════════════════════════════════════════════════════════
        option_map = destino_info.get("option_map") if isinstance(destino_info, dict) else {}
        if not isinstance(option_map, dict):
            option_map = {}

        if origin_checked_options and norm_nome in origin_checked_options:
            # ── Modo PRECISO: usar apenas labels realmente checked na ORIGEM ──
            checked_labels = origin_checked_options[norm_nome]
            matched = 0
            unmatched = []

            for label in checked_labels:
                label_clean = label.strip()
                if not label_clean:
                    continue

                # Procurar o option_id no catálogo do DESTINO pelo label
                destino_option_id = option_map.get(domain.normalize(label_clean))
                if destino_option_id:
                    option_info_entries.append(f"{destino_option_id}-{destino_id}")
                    matched += 1
                else:
                    unmatched.append(label_clean)

            logger.info(
                "    📖 MODO PRECISO para '%s': %d checked na ORIGEM → %d mapeados, %d sem match",
                nome, len(checked_labels), matched, len(unmatched),
            )
            if unmatched:
                opcoes_nao_encontradas[nome] = unmatched
                logger.warning("    ⚠️ Opções sem match no DESTINO: %s", unmatched[:10])

            opcoes_stats[nome] = {
                "mode": "precise",
                "origin_checked": len(checked_labels),
                "mapped": matched,
                "unmatched": len(unmatched),
            }

        elif opcoes_nomes:
            # ── Modo FALLBACK: usar TODAS as opções do JSON ──
            matched = 0
            for opcao_nome in opcoes_nomes:
                destino_option_id = option_map.get(domain.normalize(opcao_nome))
                if destino_option_id:
                    option_info_entries.append(f"{destino_option_id}-{destino_id}")
                    matched += 1
                else:
                    opcoes_nao_encontradas.setdefault(nome, []).append(opcao_nome)

            logger.info(
                "    ⚠️ MODO FALLBACK para '%s': %d opções do JSON → %d mapeadas (PODE MARCAR DEMAIS!)",
                nome, len(opcoes_nomes), matched,
            )
            opcoes_stats[nome] = {
                "mode": "fallback_all_json",
                "total_json": len(opcoes_nomes),
                "mapped": matched,
            }

    if not ids_desejados:
        logger.warning("⚠️ Nenhuma info para vincular")
        log_entry["infos_adicionais"] = {
            "status": "nenhum_id_encontrado",
            "nao_encontrados_no_catalogo": nao_encontrados,
        }
        return

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 2: Verificar estado atual
    # ══════════════════════════════════════════════════════════════════════
    ids_atuais = destino_api.get_product_current_infos(page, product_id, logger=logger)

    set_desejados = set(ids_desejados)
    set_atuais = set(ids_atuais)

    a_adicionar = set_desejados - set_atuais
    a_remover = set_atuais - set_desejados

    logger.info("📊 Campos: desejados=%s atuais=%s", sorted(set_desejados), sorted(set_atuais))
    logger.info("    +adicionar=%s -remover=%s", sorted(a_adicionar), sorted(a_remover))

    # Verificar se opções mudaram (mesmo que campos estejam corretos)
    fields_changed = (set_desejados != set_atuais)
    has_options = bool(option_info_entries)

    # SEMPRE enviar POST se temos option_info_entries — garante que as opções
    # sejam atualizadas mesmo quando os campos já estão corretos
    needs_post = fields_changed or has_options

    if not needs_post:
        logger.info("✅ Campos e opções já corretos — nada a fazer")
        log_entry["infos_adicionais"] = {
            "status": "ja_correto",
            "ids": sorted(set_desejados),
            "origem": source_context,
        }
        return

    # ══════════════════════════════════════════════════════════════════════
    # ETAPA 3: Enviar POST com campos + opções
    # ══════════════════════════════════════════════════════════════════════
    logger.info("📤 Enviando POST: %d campos + %d option_info entries", len(ids_desejados), len(option_info_entries))
    medium_delay()

    ok, detail = destino_api.post_additional_infos(
        page,
        product_id,
        ids_desejados,
        short_delay=short_delay,
        sort_entries=sort_entries,
        option_info_entries=option_info_entries,
    )

    if ok:
        short_delay()

        # Pós-validação dos campos
        ids_pos = destino_api.get_product_current_infos(page, product_id, logger=logger)
        set_pos = set(ids_pos)
        campos_ok = (set_pos == set_desejados)

        if campos_ok:
            logger.info("✅ Campos corretos após POST (%s)", detail)
        else:
            logger.warning("⚠️ Campos divergentes: esperado=%s atual=%s", sorted(set_desejados), sorted(set_pos))
            # Retry
            medium_delay()
            ok2, detail2 = destino_api.post_additional_infos(
                page, product_id, ids_desejados,
                short_delay=short_delay,
                sort_entries=sort_entries,
                option_info_entries=option_info_entries,
            )
            if ok2:
                short_delay()
                ids_pos2 = destino_api.get_product_current_infos(page, product_id, logger=logger)
                set_pos = set(ids_pos2)
                campos_ok = (set_pos == set_desejados)
                if campos_ok:
                    logger.info("✅ Campos corretos após retry")

        # Pós-validação das opções (se tínhamos modo preciso)
        opcoes_ok = True
        if origin_checked_options:
            short_delay()
            destino_checked = destino_api.get_product_current_checked_options(page, product_id, logger=logger)
            if destino_checked:
                for norm_name, expected_labels in origin_checked_options.items():
                    actual_labels = destino_checked.get(norm_name, [])
                    expected_set = {l.strip().lower() for l in expected_labels if l.strip()}
                    actual_set = {l.strip().lower() for l in actual_labels if l.strip()}
                    if expected_set != actual_set:
                        extra = actual_set - expected_set
                        missing = expected_set - actual_set
                        logger.warning(
                            "⚠️ Opções divergentes para '%s': extra=%s faltando=%s",
                            norm_name, sorted(extra)[:5], sorted(missing)[:5],
                        )
                        opcoes_ok = False
                    else:
                        logger.info("✅ Opções de '%s' corretas (%d)", norm_name, len(expected_set))

        status_final = "atualizado"
        if not campos_ok:
            status_final = "atualizado_com_divergencia_campos"
        elif not opcoes_ok:
            status_final = "atualizado_com_divergencia_opcoes"

        log_entry["infos_adicionais"] = {
            "status": status_final,
            "ids_vinculados": ids_desejados,
            "campos_adicionados": sorted(a_adicionar),
            "campos_removidos": sorted(a_remover),
            "option_info_enviado": len(option_info_entries),
            "opcoes_stats": opcoes_stats,
            "nao_encontrados_no_catalogo": nao_encontrados,
            "opcoes_nao_encontradas": opcoes_nao_encontradas,
            "ids_pos_update": sorted(set_pos),
            "origem": source_context,
            "detail": detail,
        }
    else:
        logger.error("❌ Falha POST infos adicionais: %s", detail)
        log_entry["infos_adicionais"] = {
            "status": "falha",
            "detalhe": detail,
            "nao_encontrados_no_catalogo": nao_encontrados,
            "opcoes_nao_encontradas": opcoes_nao_encontradas,
            "origem": source_context,
        }