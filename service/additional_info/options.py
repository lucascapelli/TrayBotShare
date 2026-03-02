import time
import random

from .config import _options_cache
from .utils import _normalize_option_key, _is_fake_header
from .scraper import _fetch_options_from_html
from .php_forms import _create_option_via_php


def _ensure_options_for_field(page, base_url: str, field_id, desired_options: list) -> tuple[int, int, list]:
    created_total = 0
    skipped = 0
    errors = []

    existing = _fetch_options_from_html(page, field_id, base_url)
    existing_keys = set(_normalize_option_key(o) for o in existing)

    to_create = []
    for opt in desired_options:
        key = _normalize_option_key(opt)
        if not key or _is_fake_header(key) or key in existing_keys:
            skipped += 1
            continue
        to_create.append(opt)

    for opt in to_create:
        success = _create_option_via_php(page, base_url, field_id, opt)
        if success:
            created_total += 1
        else:
            errors.append({"option": opt, "error": "Falha na criação via PHP"})
        time.sleep(random.uniform(0.25, 0.45))

    if created_total:
        # Invalida cache para refletir as novas opções criadas
        _options_cache.pop(field_id, None)

    return created_total, skipped, errors
