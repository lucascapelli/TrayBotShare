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
    origin_checked_options: Optional[Dict[str, List[str]]] = None,
):
    _log_section("SYNC INFORMAÇÕES ADICIONAIS")

    if not origem_infos:
        logger.info("ℹ️ Nenhuma informação adicional para sincronizar (lista vazia)")
        log_entry["infos_adicionais"] = {"status": "lista_vazia", "source": source_context}
        return

    origem_infos = domain.fix_opcao_banho_list(origem_infos)

    info_catalog = destino_api.fetch_all_additional_infos_catalog(page, token, logger=logger)
    if not info_catalog:
        logger.warning("Catálogo de Additional Infos vazio no destino")
        if not create_missing_fields:
            log_entry["infos_adicionais"] = {"status": "catalogo_vazio"}
            return

    ids_desejados = []
    sort_entries = []
    option_info_entries = []  # "OPTION_ID-FIELD_ID"
    nao_encontrados = []
    opcoes_nao_encontradas = {}
    opcoes_stats = {}

    for info in origem_infos:
        nome = domain.fix_opcao_banho_str((info.get("nome") or "").strip())
        if not nome:
            continue

        norm_nome = domain.normalize(nome)
        destino_info = info_catalog.get(norm_nome)
        destino_id = destino_info.get("id") if destino_info else None

        # opções da origem: lista de nomes
        opcoes_origem = info.get("opcoes") or []
        opcoes_nomes = [op.get("nome", "").strip() for op in opcoes_origem if op.get("nome", "").strip()]

        # Sempre assume textual se veio sem opções reais ou só dummy
        is_textual = (
            len(opcoes_nomes) <= 1
            and (not opcoes_nomes or "Valor padrão" in opcoes_nomes[0] or opcoes_nomes[0] == "")
        )

        # Força "I" (linha de texto) para campos textuais da origem
        field_type = "I" if is_textual else "S"

        logger.info("Campo '%s': is_textual=%s → field_type=%s", nome, is_textual, field_type)

        # Criar campo + opções se permitido
        if create_missing_fields and (not destino_id or (opcoes_nomes and destino_info)):
            logger.info("Criando/garantindo campo '%s' tipo=%s com %d opções", nome, field_type, len(opcoes_nomes))
            ensured = destino_api.ensure_additional_info_with_options(
                page, token, nome, opcoes_nomes, logger=logger, field_type=field_type
            )
            if ensured:
                destino_info = ensured
                destino_id = ensured.get("id")
                info_catalog[norm_nome] = ensured

        if not destino_id:
            nao_encontrados.append(nome)
            logger.warning("Campo '%s' não existe no catálogo do destino", nome)
            continue

        ids_desejados.append(destino_id)
        sort_entries.append(f"{destino_id}-")
        logger.info("Campo '%s' → ID destino %s (tipo=%s)", nome, destino_id, field_type)

        # Se for textual e destino_id existe, pular marcação de opções
        if is_textual and destino_id:
            logger.info("Campo textual '%s' → pulando marcação de opções", nome)
            opcoes_stats[nome] = {"mapped": 0, "total_origem": len(opcoes_nomes), "tipo": "textual"}
            continue

        # Decidir quais opções marcar — campo select
        option_map = destino_info.get("option_map", {}) if destino_info else {}
        matched = 0

        if origin_checked_options and norm_nome in origin_checked_options:
            checked = origin_checked_options[norm_nome]
            logger.info("Modo preciso: '%s' → %d opções marcadas na origem", nome, len(checked))
            for label in checked:
                label_clean = label.strip()
                opt_id = option_map.get(domain.normalize(label_clean))
                if opt_id:
                    option_info_entries.append(f"{opt_id}-{destino_id}")
                    matched += 1
                else:
                    opcoes_nao_encontradas.setdefault(nome, []).append(label_clean)
        elif opcoes_nomes:
            logger.info("Modo fallback: '%s' → marcando todas %d opções do JSON", nome, len(opcoes_nomes))
            for op_nome in opcoes_nomes:
                opt_id = option_map.get(domain.normalize(op_nome))
                if opt_id:
                    option_info_entries.append(f"{opt_id}-{destino_id}")
                    matched += 1
                else:
                    opcoes_nao_encontradas.setdefault(nome, []).append(op_nome)

        opcoes_stats[nome] = {"mapped": matched, "total_origem": len(opcoes_nomes), "tipo": "select"}

    if not ids_desejados:
        logger.warning("Nenhum campo pôde ser vinculado")
        log_entry["infos_adicionais"] = {"status": "nenhum_campo_vinculado", "nao_encontrados": nao_encontrados}
        return

    logger.debug("Preparando POST: ids=%s | sort=%s | option_info=%s", ids_desejados, sort_entries, option_info_entries)
    logger.info("Enviando POST com %d campos e %d opções marcadas", len(ids_desejados), len(option_info_entries))
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
        logger.info("POST AdditionalInfos OK → %s", detail)
        # Validação pós-envio
        time.sleep(2)
        atuais = destino_api.get_product_current_infos(page, product_id, logger=logger)
        log_entry["infos_adicionais"] = {
            "status": "sucesso",
            "ids_enviados": ids_desejados,
            "ids_atuais_pos": atuais,
            "opcoes_enviadas": len(option_info_entries),
            "source": source_context,
            "stats": opcoes_stats,
            "nao_encontrados": nao_encontrados,
            "opcoes_nao_mapeadas": opcoes_nao_encontradas,
        }
    else:
        logger.error("Falha no POST AdditionalInfos: %s", detail)
        log_entry["infos_adicionais"] = {
            "status": "falha_post",
            "detail": detail,
            "ids_tentados": ids_desejados,
        }
