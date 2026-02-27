# auth.py
import os
import json
import random
import time
from patchright.sync_api import Page  # ← TROQUEI AQUI


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
    try:
        context.add_cookies(cookies)
    except Exception:
        pass
    return cookie_path


def save_cookies(context, filepath):
    try:
        cookies = context.cookies()
    except Exception:
        cookies = []
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2, ensure_ascii=False)


def needs_login(page: Page):
    try:
        url = page.url.lower()
        if "login" in url or "entrar" in url:
            return True
        if page.locator("#usuario, input[type='email'], input[name='username']").count() > 0:
            return True
        if page.locator("input[type='password']").count() > 0:
            return True
        return False
    except Exception:
        return False


def human_type(page, selector, text):
    locator = page.locator(selector)
    locator.click()
    for char in text:
        locator.press(char)
        time.sleep(random.uniform(0.07, 0.18))


def login_if_needed(context, page: Page, username, password, cookie_save_path=None):
    if not needs_login(page):
        return True

    if not username or not password:
        return False

    # espera campos
    page.wait_for_selector("#usuario, input[type='email'], input[name='username']", timeout=15000)
    page.wait_for_selector("#senha, input[type='password']", timeout=15000)

    # mouse humano
    page.mouse.move(random.randint(150, 800), random.randint(100, 600), steps=18)

    # digitação humana (Tray detecta fill rápido)
    if page.locator("#usuario").count() > 0:
        human_type(page, "#usuario", username)
    else:
        human_type(page, "input[type='email'], input[name='username']", username)

    time.sleep(random.uniform(0.8, 1.6))
    human_type(page, "#senha, input[type='password']", password)

    time.sleep(random.uniform(1.2, 2.5))

    # submit
    submit = page.locator("button[type='submit'], input[type='submit'], .btn-login")
    if submit.count() > 0:
        submit.first.click()
    else:
        page.keyboard.press("Enter")

    page.wait_for_timeout(random.randint(4000, 6500))

    # OTP
    if page.locator("#code").count() > 0:
        code = input("Digite o código OTP: ").strip()
        page.locator("#code").fill(code)
        page.locator("#code").press("Enter")
        page.wait_for_timeout(4000)

    if needs_login(page):
        return False

    if cookie_save_path:
        save_cookies(context, cookie_save_path)

    return True


def authenticate(context, url, username, password, cookie_files):
    cookie_path = load_cookies(context, cookie_files)

    page = context.new_page()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except Exception:
        page.goto(url, timeout=90000)

    page.wait_for_load_state("networkidle", timeout=15000)

    save_path = cookie_path or cookie_files[0]

    ok = login_if_needed(context, page, username, password, cookie_save_path=save_path)

    if not ok:
        page.close()
        return None

    # movimento humano extra (evita "Loja bloqueada")
    page.wait_for_timeout(random.randint(3500, 6000))
    page.mouse.move(random.randint(400, 1200), random.randint(300, 800), steps=25)

    return page