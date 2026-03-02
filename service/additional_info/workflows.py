import time
import random
import json

from .config import logger, PATCH_LIMIT, PATCH_TEST_LIMIT, REQUEST_TIMEOUT_MS
from .token import _extract_token
from .api import _fetch_all_items, _fetch_full_item, _build_existing_names, _build_base_payload
from .php_forms import _try_json_api_raw, _try_php_form
from .scraper import _fetch_options_from_html
from .options import _ensure_options_for_field
from .operations import cleanup_destination_selects, _deduplicate_origin
from .utils import _normalize_option_key, _is_fake_header


# ---------------------------------------------------------------------------
# Relatório final completo do destino
# ---------------------------------------------------------------------------
def _generate_destination_report(page, base_url: str, api_url: str, headers: dict):
    print("\n" + "=" * 70)
    print("RELATÓRIO FINAL DO DESTINO — TODAS AS INFORMAÇÕES ADICIONAIS")
    print("=" * 70)

    all_items = _fetch_all_items(page, api_url, headers)
    if not all_items:
        print("Nenhum item encontrado no destino.")
        return

    print(f"Total de informações adicionais no destino: {len(all_items)}\n")

    by_type = {}
    active_count = 0
    required_count = 0

    for item in all_items:
        t = item.get("type", "?")
        by_type[t] = by_type.get(t, 0) + 1
        if str(item.get("active", "0")) == "1":
            active_count += 1
        if str(item.get("required", "0")) == "1":
            required_count += 1

    print("ESTATÍSTICAS GERAIS:")
    print(f"   Total:        {len(all_items)}")
    print(f"   Ativos:       {active_count}")
    print(f"   Obrigatórios: {required_count}")
    print(f"   Por tipo:")
    for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"      {t}: {n}")
    print("-" * 70)

    report_data = []
    selects_com_opcoes = 0
    selects_sem_opcoes = 0
    total_opcoes = 0

    for idx, item in enumerate(all_items, 1):
        item_id = item.get("id")
        name = item.get("custom_name") or item.get("name") or "?"
        tipo = item.get("type", "?")
        active = "Sim" if str(item.get("active", "0")) == "1" else "Não"
        required = "Sim" if str(item.get("required", "0")) == "1" else "Não"
        value = item.get("value", "")
        display_value = item.get("display_value", "0")
        add_total = item.get("add_total", "1")
        order = item.get("order", "0")
        max_length = item.get("max_length", "0")

        print(f"\n[{idx:03d}] {name}")
        print(f"      ID: {item_id} | Tipo: {tipo} | Ativo: {active} | Obrigatório: {required}")
        print(f"      DADOS GERAIS -> value={value} | display_value={display_value} | "
              f"add_total={add_total} | order={order} | max_length={max_length}")

        options_info = []
        if tipo == "select":
            real_options = _fetch_options_from_html(page, item_id, base_url)
            if real_options:
                selects_com_opcoes += 1
                total_opcoes += len(real_options)
                print(f"      VALORES ({len(real_options)} opções):")
                for oi, opt in enumerate(real_options, 1):
                    opt_name = opt.get("value", "?")
                    opt_price = opt.get("price", "0.00")
                    print(f"         {oi}. {opt_name} -> R$ {opt_price}")
                    options_info.append({"value": opt_name, "price": opt_price})
            else:
                selects_sem_opcoes += 1
                print(f"      VALORES: NENHUMA OPÇÃO (vazio!)")
        else:
            print(f"      VALORES: (tipo {tipo} — não usa opções)")

        report_data.append({
            "id": item_id, "name": name, "type": tipo, "active": active,
            "required": required, "value": value, "display_value": display_value,
            "add_total": add_total, "order": order, "max_length": max_length,
            "options": options_info, "options_count": len(options_info),
        })
        time.sleep(random.uniform(0.1, 0.3))

    print("\n" + "=" * 70)
    print("RESUMO FINAL DO DESTINO:")
    print(f"   Total informações adicionais: {len(all_items)}")
    print(f"   Selects COM opções:           {selects_com_opcoes}")
    print(f"   Selects SEM opções (vazios!):  {selects_sem_opcoes}")
    print(f"   Total de opções somadas:       {total_opcoes}")
    print(f"   Outros tipos (text/textarea):  {len(all_items) - selects_com_opcoes - selects_sem_opcoes}")
    print("=" * 70)

    try:
        with open("produtos/destino_report.json", "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_items": len(all_items),
                "stats": {
                    "by_type": by_type, "active": active_count, "required": required_count,
                    "selects_com_opcoes": selects_com_opcoes, "selects_sem_opcoes": selects_sem_opcoes,
                    "total_opcoes": total_opcoes,
                },
                "items": report_data,
            }, f, indent=2, ensure_ascii=False)
        print(f"Relatório salvo em: produtos/destino_report.json")
    except Exception as e:
        print(f"Erro ao salvar relatório: {e}")

    return report_data


# ---------------------------------------------------------------------------
# Coleta da ORIGEM
# ---------------------------------------------------------------------------
def collect_all_additional_info(page, storage):
    print("\n" + "=" * 70)
    print("COLETANDO INFORMAÇÕES ADICIONAIS DA ORIGEM")
    print("=" * 70)

    base_url = "https://www.grasiely.com.br/admin/api/additional-info"
    page_url = "https://www.grasiely.com.br/admin/products/additional-info"

    try:
        page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(random.randint(1500, 2500))
        print("Página carregada\n")
    except Exception as e:
        print(f"Erro ao carregar página: {e}")
        return []

    token = _extract_token(page, "grasiely.com.br")
    if not token:
        print("Não foi possível obter token. Abortando.")
        return []

    print(f"Token: {token[:35]}...")

    headers = {
        "Authorization": token,
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }

    all_data = []
    page_num = 1
    max_pages = 50

    try:
        initial_url = f"{base_url}?sort=id&page[size]=25&page[number]=1"
        response = page.request.get(initial_url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
        if response.status != 200:
            print(f"Erro: Status {response.status}")
            return []

        data = response.json()
        total_records = data.get("paging", {}).get("total", 0)
        total_pages = (total_records + 24) // 25

        print(f"Total: {total_records} registros, {total_pages} páginas")
        items = data.get("data", [])
        all_data.extend(items)
        print(f"  Página 1/{total_pages}: +{len(items)} (total: {len(all_data)})")
        page_num = 2
        max_pages = min(max_pages, total_pages)
    except Exception as e:
        print(f"Erro: {e}")
        return []

    while page_num <= max_pages:
        url = f"{base_url}?sort=id&page[size]=25&page[number]={page_num}"
        try:
            response = page.request.get(url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
            if response.status != 200:
                break
            data = response.json()
            items = data.get("data", [])
            if not items:
                break
            all_data.extend(items)
            print(f"  Página {page_num}/{total_pages}: +{len(items)} (total: {len(all_data)})")
            page_num += 1
            time.sleep(random.uniform(0.4, 0.8))
        except Exception as e:
            print(f"  Página {page_num}: {e}")
            break

    print("-" * 70)
    print("Verificando selects individualmente para capturar options...")
    for item in all_data:
        if item.get("type") == "select" and not item.get("options"):
            item_id = item.get("id")
            if not item_id:
                continue
            full = _fetch_full_item(page, base_url, headers, item_id)
            if full:
                item["options"] = full.get("options") or []

    print("-" * 70)
    print("FORÇANDO scrape via HTML das opções na ORIGEM...")
    origin_php_base = "https://www.grasiely.com.br"
    scraped = 0
    for item in all_data:
        if item.get("type") == "select":
            field_id = item.get("id")
            if not field_id:
                continue
            real_options = _fetch_options_from_html(page, field_id, origin_php_base)
            if real_options:
                item["options"] = real_options
                scraped += 1
                ex = real_options[0]
                print(f"   id={field_id} -> {len(real_options)} opções reais (ex: '{ex['value']}' R${ex['price']})")
            else:
                print(f"   id={field_id} -> 0 opções (HTML falhou)")

    print(f"Scrape HTML ORIGEM finalizado! {scraped} selects corrigidos.")

    print("-" * 70)
    print("DEDUPLICANDO origem...")
    all_data = _deduplicate_origin(all_data)

    print(f"COLETA FINALIZADA! Total (deduplicado): {len(all_data)}")
    print("=" * 70)

    storage.save_many(all_data)
    return all_data


# ---------------------------------------------------------------------------
# Sync para DESTINO
# ---------------------------------------------------------------------------
def sync_additional_info_to_destino(page, data_list):
    print("\n" + "=" * 70)
    print(f"SINCRONIZANDO {len(data_list)} INFORMAÇÕES ADICIONAIS PARA O DESTINO")
    print("=" * 70)

    site_base = "https://www.grasielyatacado.com.br"
    api_url = f"{site_base}/admin/api/additional-info"
    page_url = f"{site_base}/admin/products/additional-info"

    try:
        page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(random.randint(1500, 2500))
        print("Página carregada")
    except Exception as e:
        print(f"Erro ao carregar página: {e}")
        return

    token = _extract_token(page, "grasielyatacado.com.br")
    if not token:
        print("Não foi possível obter token. Abortando.")
        return

    print(f"Token: {token[:35]}...")

    headers_get = {
        "Authorization": token,
        "Accept": "application/json, text/plain, */*",
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
    }
    headers_post = {**headers_get, "Content-Type": "application/json"}

    # LIMPEZA TURBO
    cleanup_result = cleanup_destination_selects(page, api_url, headers_get, site_base)

    try:
        page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1000)
    except Exception:
        pass

    print("\nBuscando itens já cadastrados no DESTINO...")
    existing = _fetch_all_items(page, api_url, headers_get)
    existing_names = _build_existing_names(existing)

    existing_by_name = {}
    for e_item in existing:
        ename = (e_item.get("custom_name") or e_item.get("name") or "").strip().lower()
        if ename:
            if ename not in existing_by_name:
                existing_by_name[ename] = []
            existing_by_name[ename].append(e_item)

    print(f"{len(existing)} itens já existem no DESTINO")

    to_sync = []
    to_merge = []
    skipped = 0

    origem_map = {
        (i.get("custom_name") or i.get("name") or "").strip().lower(): i
        for i in data_list
    }

    for item in data_list:
        name = (item.get("custom_name") or item.get("name") or "").strip().lower()
        if name in existing_names:
            to_merge.append(item)
            skipped += 1
        else:
            to_sync.append(item)

    print(f"{skipped} já existem -> serão verificados para MERGE de opções")
    print(f"{len(to_sync)} precisam ser criados do zero")

    success = 0
    errors = 0
    error_list = []

    # Parte 1: Criar novos
    if to_sync:
        texts_total = sum(1 for i in to_sync if i.get("type") != "select")
        selects_total = sum(1 for i in to_sync if i.get("type") == "select")
        print(f"\n   Novos: {texts_total} text/textarea + {selects_total} select")
        print("-" * 70)

        for idx, item in enumerate(to_sync, 1):
            display_name = item.get("custom_name") or item.get("name", "?")
            tipo = item.get("type", "text")
            print(f"[{idx:03d}/{len(to_sync)}] {display_name} (tipo: {tipo})", end="")

            payload = _build_base_payload(item)
            resp = _try_json_api_raw(page, api_url, headers_post, payload)

            created_id = None
            if resp and resp.status in (200, 201):
                try:
                    json_body = resp.json()
                    created_id = (json_body.get("data") or {}).get("id") or json_body.get("id")
                except Exception:
                    pass

            if resp and resp.status in (200, 201):
                success += 1
                print(f" -> OK API ({resp.status})", end="")
            else:
                print(f" -> API falhou ({getattr(resp, 'status', 0)}), tentando PHP...", end="")
                ok2, status2, body2 = _try_php_form(page, site_base, item)
                if ok2:
                    fresh = _fetch_all_items(page, api_url, headers_get)
                    name_key = (item.get("custom_name") or item.get("name") or "").strip().lower()
                    match = next((x for x in fresh if (x.get("custom_name") or x.get("name") or "").strip().lower() == name_key), None)
                    if match:
                        created_id = match.get("id")
                    success += 1
                    print(f" OK PHP ({status2})", end="")
                else:
                    errors += 1
                    print(f" FALHOU ambos", end="")
                    body = ""
                    try:
                        body = resp.text()[:150]
                    except Exception:
                        pass
                    error_list.append({
                        "name": display_name, "type": tipo,
                        "api_status": getattr(resp, "status", 0), "api_body": body,
                        "php_status": status2, "php_body": body2, "payload": payload,
                    })

            if created_id and item.get("type") == "select":
                options = item.get("options") or []
                if options:
                    print(f" (ex: {options[0].get('value')} - R$ {options[0].get('price')})", end="")
                created_count, skipped_count, opt_errors = _ensure_options_for_field(page, site_base, created_id, options)
                if created_count:
                    print(f" +{created_count} opts", end="")
                if skipped_count:
                    print(f" ({skipped_count} já)", end="")
                if opt_errors:
                    errors += len(opt_errors)
                    error_list.extend([{"name": display_name, "type": "option_error", "detail": e} for e in opt_errors])

            print("")
            time.sleep(random.uniform(0.4, 0.8))

    # Parte 2: MERGE
    print("\n" + "-" * 70)
    print("MERGE: Verificando itens existentes no destino...")
    print("-" * 70)

    merged = 0
    merge_skipped = 0

    for item in to_merge:
        name_key = (item.get("custom_name") or item.get("name") or "").strip().lower()
        tipo = item.get("type", "text")

        if tipo != "select":
            merge_skipped += 1
            continue

        origem_options = item.get("options") or []
        if not origem_options:
            merge_skipped += 1
            continue

        dest_items = existing_by_name.get(name_key, [])
        if not dest_items:
            merge_skipped += 1
            continue

        for dest_item in dest_items:
            dest_id = dest_item.get("id")
            if not dest_id:
                continue

            print(f"[MERGE] '{name_key}' (dest_id={dest_id})", end=" ")

            dest_options = _fetch_options_from_html(page, dest_id, site_base)
            dest_count = len(dest_options)
            origem_real_opts = [o for o in origem_options if _normalize_option_key(o) and not _is_fake_header(_normalize_option_key(o))]

            if dest_count >= len(origem_real_opts):
                print(f"-> destino já tem {dest_count} opts (origem {len(origem_real_opts)}) — OK")
                merge_skipped += 1
                continue

            print(f"-> destino {dest_count} opts, origem {len(origem_real_opts)} — COMPLETANDO")

            created_count, skip_count, opt_errors = _ensure_options_for_field(page, site_base, dest_id, origem_options)
            if created_count:
                print(f"   +{created_count} opções adicionadas")
                merged += created_count
            if opt_errors:
                errors += len(opt_errors)
                error_list.extend([{"name": name_key, "type": "merge_error", "detail": e} for e in opt_errors])

            time.sleep(random.uniform(0.2, 0.4))

    print(f"\n   Merge: {merged} opções adicionadas | {merge_skipped} não precisaram")

    # Parte 3: Patch selects existentes
    print("-" * 70)
    print("Corrigindo selects existentes no DESTINO (patch final)...")
    raw_existing = _fetch_all_items(page, api_url, headers_get)
    selects_destino = [e for e in raw_existing if e.get("type") == "select"]

    patched = 0
    total_to_process = min(len(selects_destino), PATCH_LIMIT)
    if PATCH_TEST_LIMIT and PATCH_TEST_LIMIT > 0:
        total_to_process = min(total_to_process, PATCH_TEST_LIMIT)

    for i, dest_stub in enumerate(selects_destino[:total_to_process], 1):
        dest_id = dest_stub.get("id")
        name_key = (dest_stub.get("custom_name") or dest_stub.get("name") or "").strip().lower()
        print(f"[PATCH] {i}/{total_to_process} id={dest_id} name='{name_key}'", end=" ")

        dest_full = _fetch_full_item(page, api_url, headers_get, dest_id)
        if not dest_full:
            print("-> falha detalhe (skip)")
            continue

        origem_item = origem_map.get(name_key)
        if not origem_item:
            print("-> sem origem (skip)")
            continue

        origem_options = origem_item.get("options") or []
        if not origem_options:
            print("-> origem sem options (skip)")
            continue

        if origem_options:
            print(f"(ex: {origem_options[0].get('value')} R${origem_options[0].get('price')})", end=" ")

        created_count, skipped_count, opt_errors = _ensure_options_for_field(page, site_base, dest_id, origem_options)
        if created_count:
            print(f"-> +{created_count} criadas", end="")
            patched += created_count
        elif skipped_count:
            print("-> já OK", end="")
        else:
            print("-> nada", end="")

        if opt_errors:
            errors += len(opt_errors)
            error_list.extend([{"name": name_key, "type": "option_error", "detail": e} for e in opt_errors])

        print("")
        time.sleep(random.uniform(0.1, 0.3))

    # Parte 4: RELATÓRIO FINAL
    _generate_destination_report(page, site_base, api_url, headers_get)

    print("\n" + "=" * 70)
    print("SINCRONIZAÇÃO + MERGE + RELATÓRIO FINALIZADOS!")
    print(f"   LIMPEZA:")
    print(f"      Duplicatas deletadas:     {cleanup_result.get('dupes_deleted', 0)}")
    print(f"      Mojibakes corrigidos:     {cleanup_result.get('mojibake_fixed', 0)}")
    print(f"   SYNC:")
    print(f"   Total origem (deduplicado): {len(data_list)}")
    print(f"   Já existiam:               {skipped}")
    print(f"   Criados novos:             {success}")
    print(f"   Merge options adicionadas:  {merged}")
    print(f"   Patch options adicionadas:  {patched}")
    print(f"   Erros:                     {errors}")

    if error_list:
        try:
            with open("produtos/sync_errors.json", "w", encoding="utf-8") as f:
                json.dump(error_list, f, indent=2, ensure_ascii=False)
            print(f"\n   Erros em: produtos/sync_errors.json")
        except Exception:
            pass

    print("=" * 70)
