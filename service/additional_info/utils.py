import re
import logging

from .config import FAKE_HEADER_VALUES, logger


def _is_fake_header(value: str) -> bool:
    """Verifica se o texto raspado é um header do formulário Tray, não uma opção real."""
    clean = value.strip().lower()
    return clean in FAKE_HEADER_VALUES


def _fix_mojibake(text: str) -> str:
    if not text:
        return text

    mojibake_patterns = [
        "Ã§", "Ã£", "Ã¡", "Ã©", "Ã­", "Ã³", "Ãº",
        "Ã¢", "Ãª", "Ã´", "Ã¼", "Ã", "Ã"
    ]

    has_mojibake = any(p in text for p in mojibake_patterns)
    if not has_mojibake:
        return text

    try:
        fixed = text.encode('latin-1').decode('utf-8')
        logger.debug(f"Mojibake fix: '{text}' -> '{fixed}'")
        return fixed
    except (UnicodeDecodeError, UnicodeEncodeError):
        try:
            fixed = text.encode('cp1252').decode('utf-8')
            return fixed
        except (UnicodeDecodeError, UnicodeEncodeError):
            return text


def _normalize_option_key(opt: dict) -> str:
    if not opt:
        return ""
    v = opt.get("value") or opt.get("label") or ""
    v = _fix_mojibake(str(v))
    return v.strip().lower()
