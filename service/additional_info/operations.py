from .config import logger, _options_cache, _options_with_ids_cache
from .utils import _normalize_option_key, _is_fake_header
from .api import _fetch_all_items, _build_existing_names
from .scraper import _fetch_options_with_ids_from_html, _fetch_options_from_html
from .php_forms import _delete_option_via_php, _create_option_via_php, _edit_option_via_php, _edit_option_via_ui_form


def _is_option_value_persisted(page, base_url: str, field_id: int, option_id: int, expected_value: str) -> bool:
    _options_with_ids_cache.pop(field_id, None)
    fresh = _fetch_options_with_ids_from_html(page, field_id, base_url)
    for opt in fresh:
        if str(opt.get("option_id")) == str(option_id):
            current = (opt.get("value") or "").strip()
            return current == (expected_value or "").strip()
    return False


def _has_option_value(page, base_url: str, field_id: int, expected_value: str) -> bool:
    _options_with_ids_cache.pop(field_id, None)
    fresh = _fetch_options_with_ids_from_html(page, field_id, base_url)
    target = (expected_value or "").strip()
    return any((opt.get("value") or "").strip() == target for opt in fresh)


# ---------------------------------------------------------------------------
# LIMPEZA TURBO
# ---------------------------------------------------------------------------
def cleanup_destination_selects(page, api_url: str, headers: dict, base_url: str):
    print("\n🧹 LIMPEZA TURBO INICIADA — vai voar agora!")
    all_items = _fetch_all_items(page, api_url, headers)
    selects = [item for item in all_items if item.get("type") == "select"]
    print(f"{len(selects)} selects para verificar\n")

    total_deleted = 0
    total_fixed = 0

    for idx, item in enumerate(selects, 1):
        field_id = item.get("id")
        name = item.get("custom_name") or item.get("name") or "?"
        print(f"[{idx:03d}/{len(selects)}] {name} → verificando...", end=" ")

        raw_options = _fetch_options_with_ids_from_html(page, field_id, base_url)

        if not raw_options:
            print("OK")
            continue

        # ---- Passo 1: agrupar por nome normalizado para detectar duplicatas ----
        by_normalized = {}
        for opt in raw_options:
            key = opt["value_fixed"].strip().lower()
            by_normalized.setdefault(key, []).append(opt)

        to_delete_ids = []

        # Duplicatas: manter a mais correta, deletar o resto
        for group in by_normalized.values():
            if len(group) > 1:
                # Prioriza: value == value_fixed (sem mojibake) → fica; resto → deleta
                group.sort(key=lambda x: 10 if x["value"] == x["value_fixed"] else 0, reverse=True)
                for dupe in group[1:]:
                    if dupe.get("option_id"):
                        to_delete_ids.append(dupe["option_id"])

        # ---- Passo 2: detectar entradas mojibake solitárias ----
        # São opções onde value != value_fixed e não existe outra com o mesmo value_fixed
        mojibake_solo = [
            opt for opt in raw_options
            if opt["value"] != opt["value_fixed"]
            and len(by_normalized.get(opt["value_fixed"].strip().lower(), [])) == 1
            and opt.get("option_id") not in to_delete_ids
        ]

        actions = []
        if to_delete_ids:
            actions.append(f"{len(to_delete_ids)} duplicatas")
        if mojibake_solo:
            actions.append(f"{len(mojibake_solo)} mojibake")

        if not to_delete_ids and not mojibake_solo:
            print("OK (sem lixo)")
            continue

        print(f"→ corrigindo: {', '.join(actions)}...")

        # Deletar duplicatas
        for opt_id in to_delete_ids:
            if _delete_option_via_php(page, base_url, field_id, opt_id):
                total_deleted += 1
                print("✓", end="")
            else:
                print("✗", end="")

        # Corrigir mojibake: editar a opção existente com o nome correto (mesmo endpoint, id_opcao real)
        for opt in mojibake_solo:
            correct_name = opt["value_fixed"]
            opt_id = opt.get("option_id")
            print(f"\n   mojibake '{opt['value']}' → '{correct_name}'", end=" ")
            if not opt_id:
                print("✗ sem id (skip)", end="")
                continue
            edited = _edit_option_via_php(page, base_url, field_id, opt_id, {
                "value": correct_name,
                "price": opt.get("price", "0.00"),
            })
            persisted = edited and _is_option_value_persisted(page, base_url, field_id, opt_id, correct_name)

            if not persisted:
                print("↻ fallback-form", end="")
                fallback_ok = _edit_option_via_ui_form(page, base_url, field_id, opt_id, {
                    "value": correct_name,
                    "price": opt.get("price", "0.00"),
                })
                persisted = fallback_ok and _is_option_value_persisted(page, base_url, field_id, opt_id, correct_name)

            if not persisted:
                print("↻ create+delete", end="")
                created = _create_option_via_php(page, base_url, field_id, {
                    "value": correct_name,
                    "price": opt.get("price", "0.00"),
                })
                created_ok = created and _has_option_value(page, base_url, field_id, correct_name)
                if created_ok:
                    deleted_old = _delete_option_via_php(page, base_url, field_id, opt_id)
                    old_gone = not _is_option_value_persisted(page, base_url, field_id, opt_id, opt.get("value", ""))
                    persisted = deleted_old and old_gone

            if persisted:
                total_fixed += 1
                print("✓ editado", end="")
            else:
                print("✗ não persistiu", end="")

        # Invalida cache
        _options_cache.pop(field_id, None)
        _options_with_ids_cache.pop(field_id, None)

        print("")

    print(f"\n✅ LIMPEZA TURBO CONCLUÍDA! {total_deleted} duplicatas removidas | {total_fixed} mojibakes corrigidos")
    return {"dupes_deleted": total_deleted, "mojibake_fixed": total_fixed}


# ---------------------------------------------------------------------------
# Deduplicação inteligente da origem
# ---------------------------------------------------------------------------
def _deduplicate_origin(data_list: list) -> list:
    by_name = {}
    for item in data_list:
        name = (item.get("custom_name") or item.get("name") or "").strip().lower()
        if not name:
            continue
        if name not in by_name:
            by_name[name] = []
        by_name[name].append(item)

    deduped = []
    duplicates_found = 0

    for name, items in by_name.items():
        if len(items) == 1:
            deduped.append(items[0])
            continue

        duplicates_found += 1

        def _score(item):
            score = 0
            opts = item.get("options") or []
            real_opts = [o for o in opts if _normalize_option_key(o) and not _is_fake_header(_normalize_option_key(o))]
            score += len(real_opts) * 10
            priced_opts = [o for o in real_opts if o.get("price", "0.00") != "0.00"]
            score += len(priced_opts) * 5
            val = str(item.get("value") or "").strip()
            if val and val != "0.00" and val != "0":
                score += 3
            return score

        items.sort(key=_score, reverse=True)
        best = items[0]
        print(f"   DUPLICATA '{name}': {len(items)} cópias -> escolhido id={best.get('id')} (score={_score(best)}, {len(best.get('options', []))} opts)")
        deduped.append(best)

    if duplicates_found:
        print(f"\n   Deduplicação: {duplicates_found} nomes duplicados resolvidos")
        print(f"   {len(data_list)} itens origem -> {len(deduped)} após dedup\n")
    else:
        print(f"   Nenhuma duplicata encontrada na origem ({len(deduped)} itens)\n")

    return deduped


# ---------------------------------------------------------------------------
# Comparação
# ---------------------------------------------------------------------------
def compare_additional_info(origem_data: list, destino_data: list) -> list:
    destino_names = _build_existing_names(destino_data)
    return [
        item for item in origem_data
        if (item.get("custom_name") or item.get("name") or "").strip().lower() not in destino_names
    ]
