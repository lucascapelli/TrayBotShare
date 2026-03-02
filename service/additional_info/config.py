import logging

logger = logging.getLogger("additional_info")

# ====================== CACHE GLOBAL TURBO ======================
_options_cache: dict = {}
_options_with_ids_cache: dict = {}

# Ajustes operacionais
REQUEST_TIMEOUT_MS = 10000
FETCH_RETRIES = 2
CREATE_RETRIES = 2
PATCH_TEST_LIMIT = 0
PATCH_LIMIT = 999

# ---------------------------------------------------------------------------
# Headers falsos que o HTML da Tray inclui como linhas da tabela
# ---------------------------------------------------------------------------
FAKE_HEADER_VALUES = {
    "tipo",
    "forma de exibição das imagens",
    "forma de exibiÃ§Ã£o das imagens",
    "forma de exibicao das imagens",
    "limite de caracteres",
    "preço",
    "preÃ§o",
    "preco",
    "altura",
    "ativo",
    "obrigatório",
    "obrigatorio",
    "obrigatÃ³rio",
    "ordem",
    "exibir valor",
    "nome para o administrador",
    "nome para a loja",
}
