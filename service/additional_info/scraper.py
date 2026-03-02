import re

from .config import _options_cache, _options_with_ids_cache, logger
from .utils import _fix_mojibake, _is_fake_header


# ---------------------------------------------------------------------------
# JS que extrai APENAS opções reais
# ---------------------------------------------------------------------------
_JS_EXTRACT_OPTIONS = """
(() => {
    const results = [];
    const rows = document.querySelectorAll('table tbody tr');

    for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) continue;

        let optionId = null;

        const links = row.querySelectorAll('a[href]');
        for (const link of links) {
            const href = link.getAttribute('href') || '';
            const m = href.match(/id_opcao=(\\d+)/);
            if (m) { optionId = parseInt(m[1]); break; }
        }

        if (!optionId) {
            const elems = row.querySelectorAll('a, button, span, i, img');
            for (const el of elems) {
                const onclick = el.getAttribute('onclick') || '';
                let m = onclick.match(/excluir\\w*\\((\\d+)/);
                if (m) { optionId = parseInt(m[1]); break; }
                m = onclick.match(/id_opcao[\\s"':=]+(\\d+)/);
                if (m) { optionId = parseInt(m[1]); break; }
            }
        }

        const name = cells[0]?.innerText?.trim() || '';
        const priceText = cells[1]?.innerText?.trim() || '';

        if (!name || name.toLowerCase() === 'sem imagem' || name.toLowerCase() === 'nenhuma') continue;

        results.push({
            value: name,
            price: priceText,
            option_id: optionId
        });
    }
    return results;
})()
"""


# ---------------------------------------------------------------------------
# NAVEGAÇÃO TURBO (sem FrameLocator error + cache)
# ---------------------------------------------------------------------------
def _navigate_to_options_page(page, field_id: int, base_url: str):
    """Versão ultra-rápida + cache implícito"""
    if field_id in _options_cache:
        return None, True  # sinaliza que usa cache

    admin_url = f"{base_url}/admin/#/adm/extras/informacao_produto_index.php?aba=opcoes&id={field_id}"
    page.goto(admin_url, wait_until="domcontentloaded", timeout=18000)
    page.wait_for_timeout(900)

    frame = page.frame(name="centro")
    if frame:
        try:
            frame.wait_for_selector("table tbody tr", timeout=7000)
            return frame, True
        except Exception:
            pass

    direct_url = f"{base_url}/adm/extras/informacao_produto_index.php?id={field_id}&aba=opcoes"
    page.goto(direct_url, wait_until="domcontentloaded", timeout=18000)
    page.wait_for_timeout(800)
    return page, False


# ---------------------------------------------------------------------------
# FETCH COM CACHE (rápido)
# ---------------------------------------------------------------------------
def _fetch_options_from_html(page, field_id: int, base_url: str) -> list:
    if field_id in _options_cache:
        return _options_cache[field_id].copy()

    try:
        frame, _ = _navigate_to_options_page(page, field_id, base_url)
        if frame is None:
            return _options_cache[field_id]

        raw_js_results = frame.evaluate(_JS_EXTRACT_OPTIONS)

        if not raw_js_results:
            _options_cache[field_id] = []
            return []

        raw_options = []
        for item in raw_js_results:
            nome_raw = item.get("value", "").strip()
            if not nome_raw:
                continue

            nome = _fix_mojibake(nome_raw)

            price_text = item.get("price", "0.00")
            price = "0.00"
            if price_text:
                clean = re.sub(r'[^\d.,]', '', price_text).replace(',', '.').strip()
                try:
                    price = f"{float(clean):.2f}"
                except ValueError:
                    pass

            raw_options.append({
                "value": nome,
                "price": price,
                "order": len(raw_options),
            })

        seen = set()
        deduped = []
        for opt in raw_options:
            key = opt["value"].strip().lower()
            if key not in seen and not _is_fake_header(key):
                seen.add(key)
                deduped.append(opt)

        _options_cache[field_id] = deduped
        logger.info(f"Raspadas {len(deduped)} opções reais para id={field_id}")
        return deduped

    except Exception as e:
        logger.error(f"Erro scraping opções id={field_id}: {e}")
        _options_cache[field_id] = []
        return []


def _fetch_options_with_ids_from_html(page, field_id: int, base_url: str) -> list:
    if field_id in _options_with_ids_cache:
        return _options_with_ids_cache[field_id].copy()

    try:
        frame, _ = _navigate_to_options_page(page, field_id, base_url)
        if frame is None:
            return _options_with_ids_cache[field_id]

        raw_js_results = frame.evaluate(_JS_EXTRACT_OPTIONS)

        if not raw_js_results:
            return []

        options = []
        for i, item in enumerate(raw_js_results):
            nome_raw = item.get("value", "").strip()
            if not nome_raw:
                continue

            price_text = item.get("price", "0.00")
            price = "0.00"
            if price_text:
                clean = re.sub(r'[^\d.,]', '', price_text).replace(',', '.').strip()
                try:
                    price = f"{float(clean):.2f}"
                except ValueError:
                    pass

            options.append({
                "value": nome_raw,
                "value_fixed": _fix_mojibake(nome_raw),
                "price": price,
                "order": i,
                "option_id": item.get("option_id"),
            })

        _options_with_ids_cache[field_id] = options
        return options

    except Exception as e:
        logger.error(f"Erro scraping opções com IDs id={field_id}: {e}")
        return []
