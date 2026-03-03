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
):
    _log_section("SYNC INFORMAÇÕES ADICIONAIS")

    if not origem_infos:
        logger.info("ℹ️ Produto ORIGEM não tem informações adicionais — pulando")
        log_entry["infos_adicionais"] = {"status": "sem_infos_origem"}
        return

    origem_infos = domain.fix_opcao_banho_list(origem_infos)

    info_map = destino_api.fetch_all_additional_infos(page, token, logger=logger)
    if not info_map:
        logger.warning("⚠️ Catálogo de infos do DESTINO vazio — não é possível sincronizar")
        log_entry["infos_adicionais"] = {"status": "catalogo_vazio"}
        return

    ids_desejados = []
    nao_encontrados = []

    for info in origem_infos:
        nome = domain.fix_opcao_banho_str((info.get("nome") or "").strip())
        destino_id = info_map.get(domain.normalize(nome))

        if destino_id:
            ids_desejados.append(destino_id)
            logger.info("    ✅ '%s' → DESTINO ID %s", nome, destino_id)
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
    )

    if ok:
        logger.info("✅ Infos adicionais atualizadas com sucesso (%s)", detail)
        log_entry["infos_adicionais"] = {
            "status": "atualizado",
            "ids_vinculados": ids_desejados,
            "adicionados": sorted(a_adicionar),
            "removidos": sorted(a_remover),
            "nao_encontrados_no_catalogo": nao_encontrados,
        }
    else:
        logger.error("❌ Falha ao atualizar infos adicionais: %s", detail)
        log_entry["infos_adicionais"] = {
            "status": "falha",
            "detalhe": detail,
            "nao_encontrados_no_catalogo": nao_encontrados,
        }
