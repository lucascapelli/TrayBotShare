# main.py
# Versão refatorada: contextos isolados por loja, logging estruturado,
# pausas de segurança entre autenticações, cleanup correto.

import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Optional

from dotenv import load_dotenv
from patchright.sync_api import sync_playwright, Page, BrowserContext, Browser

from service.auth import authenticate, load_storage_state, _resolve_state_path
from service.scraper import collect_all_products as collect_origem
from service.scraperDestino import collect_all_products as collect_destino
from service.storage import JSONStorage
from service.sync import run_sync
from service.additional_info import (
    collect_all_additional_info,
    sync_additional_info_to_destino,
)

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

# ---------------------------------------------------------------------------
# Variáveis de ambiente
# ---------------------------------------------------------------------------
load_dotenv()

HEADLESS = True

ORIGEM_URL = os.getenv("ORIGEM_URL", "")
DESTINO_URL = os.getenv("DESTINO_URL", "https://www.grasielyatacado.com.br/admin/products/list")

SOURCE_USER = os.getenv("SOURCE_USER", "")
SOURCE_PASS = os.getenv("SOURCE_PASS", "")
TARGET_USER = os.getenv("TARGET_USER", "")
TARGET_PASS = os.getenv("TARGET_PASS", "")

COOKIES_ORIGEM = ["cookies_origem.json", "cookiesorigem.json", "cookies-origem.json"]
COOKIES_DESTINO = ["cookies_destino.json", "cookiesdestino.json", "cookies-destino.json"]

SAFETY_PAUSE_SECONDS = 5  # pausa entre autenticações distintas

# ---------------------------------------------------------------------------
# Storages
# ---------------------------------------------------------------------------
STORAGE_ORIGEM = JSONStorage(
    json_path="produtos/ProdutosOrigem.json",
    csv_path="produtos/ProdutosOrigem.csv",
    replace_on_start=False,
)

STORAGE_DESTINO = JSONStorage(
    json_path="produtos/ProdutosDestino.json",
    csv_path="produtos/ProdutosDestino.csv",
    replace_on_start=False,
)


# ---------------------------------------------------------------------------
# Gerenciamento de contexto isolado por loja
# ---------------------------------------------------------------------------
@contextmanager
def create_isolated_context(
    browser: Browser,
    cookie_files: list,
    label: str,
):
    """
    Cria um BrowserContext isolado, opcionalmente restaurando storage_state.
    Garante fechamento mesmo em caso de erro.
    """
    state_path = _resolve_state_path(cookie_files)
    stored = load_storage_state(state_path)

    kwargs = {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "pt-BR",
        "timezone_id": "America/Sao_Paulo",
    }

    if stored:
        kwargs["storage_state"] = stored
        logger.info("[%s] Contexto criado com storage_state salvo", label)
    else:
        logger.info("[%s] Contexto criado sem estado prévio", label)

    context = browser.new_context(**kwargs)
    try:
        yield context
    finally:
        try:
            context.close()
            logger.info("[%s] Contexto fechado", label)
        except Exception as exc:
            logger.warning("[%s] Erro ao fechar contexto: %s", label, exc)


def auth_in_context(
    browser: Browser,
    url: str,
    user: str,
    pwd: str,
    cookie_files: list,
    label: str,
) -> tuple[Optional[BrowserContext], Optional[Page]]:
    """
    Cria contexto isolado e autentica. Retorna (context, page) ou (None, None).
    O chamador é responsável por fechar o context quando terminar.
    """
    state_path = _resolve_state_path(cookie_files)
    stored = load_storage_state(state_path)

    kwargs = {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "pt-BR",
        "timezone_id": "America/Sao_Paulo",
    }
    if stored:
        kwargs["storage_state"] = stored
        logger.info("[%s] Restaurando sessão anterior", label)

    context = browser.new_context(**kwargs)
    page = authenticate(context, url, user, pwd, cookie_files)

    if page:
        return context, page

    # Falhou — limpar
    try:
        context.close()
    except Exception:
        pass
    return None, None


def safe_close(context: Optional[BrowserContext], label: str) -> None:
    if context is None:
        return
    try:
        context.close()
        logger.info("[%s] Contexto encerrado com sucesso", label)
    except Exception as exc:
        logger.warning("[%s] Erro ao encerrar contexto: %s", label, exc)


# ---------------------------------------------------------------------------
# Ações do menu
# ---------------------------------------------------------------------------
def action_collect_origem(browser: Browser) -> None:
    logger.info("Coletando dados da ORIGEM...")
    ctx, page = auth_in_context(
        browser, ORIGEM_URL, SOURCE_USER, SOURCE_PASS, COOKIES_ORIGEM, "ORIGEM"
    )
    if not page:
        logger.error("Autenticação na ORIGEM falhou")
        return
    try:
        collect_origem(page, STORAGE_ORIGEM)
    finally:
        safe_close(ctx, "ORIGEM")


def action_collect_destino(browser: Browser) -> None:
    logger.info("Coletando dados do DESTINO...")
    STORAGE_DESTINO.clear()
    logger.info("Arquivo ProdutosDestino.json limpo para nova coleta")

    ctx, page = auth_in_context(
        browser, DESTINO_URL, TARGET_USER, TARGET_PASS, COOKIES_DESTINO, "DESTINO"
    )
    if not page:
        logger.error("Autenticação no DESTINO falhou")
        return
    try:
        collect_destino(page, STORAGE_DESTINO)
    finally:
        safe_close(ctx, "DESTINO")


def action_sync(browser: Browser) -> None:
    logger.info("Iniciando sync ORIGEM → DESTINO (atualizar produtos no Atacado)...")
    ctx, page = auth_in_context(
        browser, DESTINO_URL, TARGET_USER, TARGET_PASS, COOKIES_DESTINO, "DESTINO"
    )
    if not page:
        logger.error("Autenticação no DESTINO falhou")
        return
    try:
        run_sync(
            ctx, STORAGE_ORIGEM, STORAGE_DESTINO,
            ORIGEM_URL, SOURCE_USER, SOURCE_PASS, COOKIES_ORIGEM,
        )
    finally:
        safe_close(ctx, "DESTINO")


def action_sync_additional(browser: Browser) -> None:
    logger.info("Sincronizando informações adicionais (Origem → Destino)...")

    storage_adicional = JSONStorage(
        json_path="produtos/InformacoesAdicionais.json",
        csv_path="produtos/InformacoesAdicionais.csv",
        replace_on_start=True,
    )

    # --- Fase 1: coletar da ORIGEM ---
    ctx_origem, page_origem = auth_in_context(
        browser, ORIGEM_URL, SOURCE_USER, SOURCE_PASS, COOKIES_ORIGEM, "ORIGEM"
    )
    if not page_origem:
        logger.error("Autenticação na ORIGEM falhou")
        return

    try:
        data_list = collect_all_additional_info(page_origem, storage_adicional)
    finally:
        safe_close(ctx_origem, "ORIGEM")

    if not data_list:
        logger.warning("Nenhum dado adicional coletado — encerrando")
        return

    logger.info("%d informações adicionais coletadas", len(data_list))

    # --- Perguntar ao usuário ---
    resposta = input("\nDeseja enviar para o DESTINO agora? (s/n): ").strip().lower()
    if resposta != "s":
        logger.info("Sincronização cancelada. Dados salvos em %s", storage_adicional.json_path)
        return

    # --- Fase 2: enviar ao DESTINO (contexto separado) ---
    logger.info("Pausa de segurança de %ds entre autenticações...", SAFETY_PAUSE_SECONDS)
    time.sleep(SAFETY_PAUSE_SECONDS)

    ctx_destino, page_destino = auth_in_context(
        browser, DESTINO_URL, TARGET_USER, TARGET_PASS, COOKIES_DESTINO, "DESTINO"
    )
    if not page_destino:
        logger.error("Autenticação no DESTINO falhou")
        return

    try:
        sync_additional_info_to_destino(page_destino, data_list)
    finally:
        safe_close(ctx_destino, "DESTINO")


# ---------------------------------------------------------------------------
# Menu principal
# ---------------------------------------------------------------------------
MENU = """
======================================================================
  TRAY BOT — ORIGEM <-> DESTINO
======================================================================
  1  Colher dados ORIGEM
  2  Colher dados DESTINO
  3  Sync ORIGEM → DESTINO (atualizar produtos)
  4  Sync Info Adicionais (origem → destino)
  0  Sair
----------------------------------------------------------------------"""


def main() -> None:
    print(MENU)
    escolha = input("Escolha (1/2/3/4/0): ").strip()

    if escolha == "0":
        print("Até mais!")
        return

    if escolha not in {"1", "2", "3", "4"}:
        print("Opção inválida.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            channel="chrome",
            args=["--no-sandbox"],
        )
        try:
            actions = {
                "1": action_collect_origem,
                "2": action_collect_destino,
                "3": action_sync,
                "4": action_sync_additional,
            }
            actions[escolha](browser)
        except KeyboardInterrupt:
            logger.info("Interrompido pelo usuário")
        except Exception as exc:
            logger.exception("Erro não tratado: %s", exc)
        finally:
            browser.close()
            logger.info("Browser encerrado")


if __name__ == "__main__":
    main()