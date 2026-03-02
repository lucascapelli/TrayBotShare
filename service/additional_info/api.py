import time
import random

from .config import REQUEST_TIMEOUT_MS, FETCH_RETRIES, logger

# ---------------------------------------------------------------------------
# Buscar itens existentes via API
# ---------------------------------------------------------------------------
def _fetch_all_items(page, api_url: str, headers: dict) -> list:
    all_items = []
    page_num = 1

    try:
        url = f"{api_url}?sort=id&page[size]=25&page[number]=1"
        response = page.request.get(url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
        if response.status != 200:
            return []
        data = response.json()
        total_records = data.get("paging", {}).get("total", 0)
        total_pages = (total_records + 24) // 25
        all_items.extend(data.get("data", []))
        page_num = 2
    except Exception as e:
        logger.error("Erro ao buscar itens: %s", e)
        return []

    while page_num <= total_pages:
        try:
            url = f"{api_url}?sort=id&page[size]=25&page[number]={page_num}"
            response = page.request.get(url, headers=headers, timeout=REQUEST_TIMEOUT_MS)
            if response.status != 200:
                break
            items = response.json().get("data", [])
            if not items:
                break
            all_items.extend(items)
            page_num += 1
            time.sleep(random.uniform(0.3, 0.6))
        except Exception:
            break

    return all_items


def _fetch_full_item(page, api_url: str, headers: dict, item_id, timeout_ms: int = REQUEST_TIMEOUT_MS) -> dict | None:
    url = f"{api_url}/{item_id}"
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            response = page.request.get(url, headers=headers, timeout=timeout_ms)
            if response.status == 200:
                data = response.json()
                item_data = data.get("data") or data
                if "options" in item_data or "values" in item_data:
                    opts = item_data.get("options") or item_data.get("values") or []
                    normalized_opts = []
                    for opt in opts:
                        if isinstance(opt, str):
                            normalized_opts.append({"value": opt, "price": "0.00", "order": 0, "add_total": 1})
                        elif isinstance(opt, dict):
                            price = opt.get("price") or opt.get("additional_price") or opt.get("valor") or "0.00"
                            normalized_opts.append({
                                "value": opt.get("value") or opt.get("label") or "",
                                "price": str(price),
                                "order": opt.get("order", 0),
                                "add_total": opt.get("add_total", 1)
                            })
                    item_data["options"] = normalized_opts
                return item_data
            else:
                return None
        except Exception:
            time.sleep(0.5 * attempt)
    return None


def _build_existing_names(items: list) -> set:
    names = set()
    for item in items:
        name = (item.get("custom_name") or item.get("name") or "").strip().lower()
        if name:
            names.add(name)
    return names


BASE_FIELDS = {
    "active", "add_total", "custom_name", "display_value",
    "max_length", "name", "order", "required", "type", "value",
}


def _build_base_payload(item: dict) -> dict:
    return {k: item[k] for k in BASE_FIELDS if k in item}
