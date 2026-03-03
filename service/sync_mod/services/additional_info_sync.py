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


def sync_additional_infos(
    page: Page,
    product_id: str,
    origem_infos: list,
    token: str,
    log_entry: dict,
    short_delay,
    medium_delay,
    create_missing_fields: bool = False,
):
    _log_section("SYNC INFORMAÇÕES ADICIONAIS")

    if not origem_infos:
        logger.info("ℹ️ Produto ORIGEM não tem informações adicionais — pulando")
        log_entry["infos_adicionais"] = {"status": "sem_infos_origem"}
        return

    origem_infos = domain.fix_opcao_banho_list(origem_infos)

    info_catalog = destino_api.fetch_all_additional_infos_catalog(page, token, logger=logger)
    if not info_catalog:
        logger.warning("⚠️ Catálogo de infos do DESTINO vazio — não é possível sincronizar")
        log_entry["infos_adicionais"] = {"status": "catalogo_vazio"}
        return

    ids_desejados = []
    sort_entries = []
    option_info_entries = []
    nao_encontrados = []

    for info in origem_infos:
        nome = domain.fix_opcao_banho_str((info.get("nome") or "").strip())
        destino_info = info_catalog.get(domain.normalize(nome))
        destino_id = destino_info.get("id") if isinstance(destino_info, dict) else None

        opcoes_origem = info.get("opcoes") or []
        opcoes_nomes = [
            (op.get("nome") or "").strip()
            for op in opcoes_origem
            if (op.get("nome") or "").strip()
        ]

        if create_missing_fields and (not destino_info or (opcoes_nomes and isinstance(destino_info, dict))):
            ensured = destino_api.ensure_additional_info_with_options(
                page,
                token,
                nome,
                opcoes_nomes,
                logger=logger,
            )
            if ensured:
                destino_info = ensured
                destino_id = ensured.get("id")
                info_catalog[domain.normalize(nome)] = ensured

        if destino_id:
            ids_desejados.append(destino_id)
            sort_entries.append(f"{destino_id}-")
            logger.info("    ✅ '%s' → DESTINO ID %s", nome, destino_id)

            option_map = destino_info.get("option_map") if isinstance(destino_info, dict) else {}
            if isinstance(option_map, dict) and opcoes_origem:
                for opcao in opcoes_origem:
                    opcao_nome = (opcao.get("nome") or "").strip()
                    if not opcao_nome:
                        continue
                    destino_option_id = option_map.get(domain.normalize(opcao_nome))
                    if destino_option_id:
                        option_info_entries.append(f"{destino_option_id}-{destino_id}")
        else:
            nao_encontrados.append(nome)
            logger.warning("    ❌ '%s' NÃO existe no catálogo DESTINO", nome)

    ids_atuais = destino_api.get_product_current_infos(page, product_id, logger=logger)

    set_desejados = set(ids_desejados)
    set_atuais = set(ids_atuais)

    a_adicionar = set_desejados - set_atuais
    a_remover = set_atuais - set_desejados
    ja_ok = set_desejados & set_atuais

    logger.info("📊 Comparação infos adicionais:")
    logger.info("    Desejadas (ORIGEM): %s", sorted(set_desejados))
    logger.info("    Atuais (DESTINO):   %s", sorted(set_atuais))
    logger.info("    Já corretas:        %s", sorted(ja_ok))
    logger.info("    A adicionar:        %s", sorted(a_adicionar))
    logger.info("    A remover:          %s", sorted(a_remover))

    if set_desejados == set_atuais:
        logger.info("✅ Informações adicionais já estão corretas — nada a fazer")
        log_entry["infos_adicionais"] = {
            "status": "ja_correto",
            "ids": sorted(set_desejados),
        }
        return

    logger.info("📤 Atualizando vínculos de infos adicionais...")
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
        logger.info("✅ Infos adicionais atualizadas com sucesso (%s)", detail)
        log_entry["infos_adicionais"] = {
            "status": "atualizado",
            "ids_vinculados": ids_desejados,
            "adicionados": sorted(a_adicionar),
            "removidos": sorted(a_remover),
            "option_info_enviado": len(option_info_entries),
            "nao_encontrados_no_catalogo": nao_encontrados,
        }
    else:
        logger.error("❌ Falha ao atualizar infos adicionais: %s", detail)
        log_entry["infos_adicionais"] = {
            "status": "falha",
            "detalhe": detail,
            "nao_encontrados_no_catalogo": nao_encontrados,
        }
