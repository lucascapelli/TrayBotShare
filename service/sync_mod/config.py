RATE_LIMIT = 0 # 0 roda sem limite, outro numero limitam a execução ao número passado
DESTINO_BASE = "https://www.grasielyatacado.com.br"

# Fonte de dados da ORIGEM:
# - "file": usa ORIGEM_JSON_PATH (comportamento atual)
# - "tray_api": preparado para leitura direta da API da Tray
ORIGEM_SOURCE = "tray_api"

ORIGEM_JSON_PATH = "produtos/ProdutosOrigem.json"

# Config de coleta direta da ORIGEM via Tray API (GET /admin/api/products/{id})
ORIGEM_TRAY_BASE = "https://www.grasiely.com.br"
ORIGEM_TRAY_PRODUCT_ENDPOINT = "/admin/api/products/{product_id}"
ORIGEM_TRAY_ACCEPT = "application/json"
ORIGEM_TRAY_MAX_PAGES = 0  # 0 = sem limite; >0 limita quantidade de páginas coletadas

# Campos esperados no JSON bruto de produto retornado pela Tray (em data)
ORIGEM_TRAY_REQUIRED_KEYS = (
	"id",
	"name",
	"price",
	"description",
	"stock",
	"AdditionalInfos",
	"Variant",
	"ProductImage",
)

# Mapeamento de chaves do JSON bruto da Tray -> modelo usado no sync
ORIGEM_TRAY_TO_SYNC_MAP = {
	"id": "produto_id",
	"name": "nome",
	"price": "preco",
	"description": "descricao",
	"stock": "estoque",
	"minimum_stock": "estoque_minimo",
	"category_name": "categoria",
	"reference": "referencia",
	"weight": "peso",
	"height": "altura",
	"width": "largura",
	"length": "comprimento",
}

MODO_TESTE_APENAS_COM_INFOS = False
LOG_FILE = "produtos/sync_log.json"
SKIP_DESTINO_PRODUCT_IDS = {"47"}
