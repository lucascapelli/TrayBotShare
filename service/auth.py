# auth.py

import os
import json
from playwright.sync_api import Page


def find_existing(path_options):
    for p in path_options:
        if os.path.isfile(p):
            return p
    return None


def normalize_cookies(raw_cookies):
    cleaned = []

    for c in raw_cookies:
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c.get("path", "/"),
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", False),
        }

        if "expirationDate" in c:
            try:
                cookie["expires"] = int(float(c["expirationDate"]))
            except Exception:
                pass

        ss = c.get("sameSite")
        if ss:
            ss = ss.lower()
            if ss == "lax":
                cookie["sameSite"] = "Lax"
            elif ss == "strict":
                cookie["sameSite"] = "Strict"
            elif ss in ("no_restriction", "none"):
                cookie["sameSite"] = "None"

        cleaned.append(cookie)

    return cleaned


def load_cookies(context, cookie_files):
    cookie_path = find_existing(cookie_files)
    if not cookie_path:
        return None

    with open(cookie_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    cookies = normalize_cookies(raw)
    context.add_cookies(cookies)

    return cookie_path


def save_cookies(context, filepath):
    cookies = context.cookies()
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2, ensure_ascii=False)


def needs_login(page: Page):
    try:
        return (
            "login" in page.url.lower()
            or page.locator("#usuario").count() > 0
            or page.locator("input[type='password']").count() > 0
        )
    except Exception:
        return False


def login_if_needed(context, page: Page, username, password, cookie_save_path=None):
    if not needs_login(page):
        return True

    if not username or not password:
        return False

    page.wait_for_selector("#usuario", timeout=15000)
    page.wait_for_selector("#senha", timeout=15000)

    page.locator("#usuario").fill(username)
    page.locator("#senha").fill(password)

    submit = page.locator(
        "button[type='submit'], input[type='submit'], .btn-login, .login-button"
    )

    if submit.count() > 0:
        submit.first.click()
    else:
        page.locator("#senha").press("Enter")

    page.wait_for_timeout(5000)

    # OTP detection
    if page.locator("#code").count() > 0:
        code = input("Digite o código OTP: ").strip()
        page.locator("#code").fill(code)
        page.locator("#code").press("Enter")
        page.wait_for_timeout(3000)

    # validação pós login
    if needs_login(page):
        return False

    if cookie_save_path:
        save_cookies(context, cookie_save_path)

    return True


def authenticate(context, url, username, password, cookie_files):
    cookie_path = load_cookies(context, cookie_files)

    page = context.new_page()
    page.goto(url)
    page.wait_for_load_state("networkidle")

    save_path = cookie_path or cookie_files[0]

    ok = login_if_needed(
        context,
        page,
        username,
        password,
        cookie_save_path=save_path
    )

    if not ok:
        page.close()
        return None

    return page