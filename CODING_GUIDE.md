# Guia de desenvolvimento - TrayBot

## Arquivos
- bot.py: controla fluxo, navegação e logs
- leitor.py: extrai dados de produtos das páginas
- comparador.py: compara produtos e gera relatórios
- escritor.py: salva logs/dados em arquivos

## Fluxo
bot.py → leitor.py → comparador.py → escritor.py

## Funções importantes
- leitor.read_fields_from_edit_page(page, FIELD_SELECTORS) → dict[str, str]
- leitor.open_search_and_open_edit(page, name, search_input, result_item, edit_button) → bool
- comparador.compare_products(origin, dest) → dict
- comparador.format_differences(report) → str