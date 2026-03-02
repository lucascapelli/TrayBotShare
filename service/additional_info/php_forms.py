import json
import time

from .config import REQUEST_TIMEOUT_MS, CREATE_RETRIES, logger


# ---------------------------------------------------------------------------
# Helper: POST via submit nativo de FORM no browser (preserva charset da página)
# ---------------------------------------------------------------------------
def _browser_post(page, url: str, form_data: dict, timeout_ms: int = REQUEST_TIMEOUT_MS) -> tuple[int, str]:
    """
    Executa POST via submit nativo de <form> dentro do contexto do browser.
    - Cookies enviados automaticamente
    - Usa o mesmo algoritmo de encoding de formulário do browser (charset da página)
    - Campo `imagem` é enviado como <input type="file" ...> vazio (igual ao admin)
    """
    fields = {k: v for k, v in form_data.items() if k != "imagem"}
    has_imagem = "imagem" in form_data

    js = """
    async ({url, fields, hasImagem, timeout}) => {
        const cleanupNodes = [];
        let done = false;
        const finish = (payload) => {
            if (done) return payload;
            done = true;
            for (const node of cleanupNodes) {
                try { node.remove(); } catch (_) {}
            }
            return payload;
        };

        return await new Promise((resolve) => {
            try {
                const uid = `traybot_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

                const iframe = document.createElement('iframe');
                iframe.name = uid;
                iframe.style.display = 'none';
                document.body.appendChild(iframe);
                cleanupNodes.push(iframe);

                const form = document.createElement('form');
                form.method = 'POST';
                form.action = url;
                form.target = uid;
                form.enctype = 'multipart/form-data';
                form.acceptCharset = document.characterSet || 'windows-1252';
                form.style.display = 'none';

                for (const [k, v] of Object.entries(fields)) {
                    const input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = k;
                    input.value = String(v ?? '');
                    form.appendChild(input);
                }

                if (hasImagem) {
                    const fileInput = document.createElement('input');
                    fileInput.type = 'file';
                    fileInput.name = 'imagem';
                    form.appendChild(fileInput);
                }

                document.body.appendChild(form);
                cleanupNodes.push(form);

                const timer = setTimeout(() => {
                    resolve(finish({status: 0, body: 'Timeout waiting form submit response'}));
                }, timeout);

                iframe.addEventListener('load', () => {
                    clearTimeout(timer);
                    try {
                        const doc = iframe.contentDocument || iframe.contentWindow?.document;
                        const text = doc?.documentElement?.outerHTML || doc?.body?.innerHTML || '';
                        resolve(finish({status: 200, body: String(text).slice(0, 300)}));
                    } catch (e) {
                        resolve(finish({status: 200, body: `loaded (body unreadable): ${String(e)}`}));
                    }
                }, { once: true });

                form.submit();
            } catch (e) {
                resolve(finish({status: 0, body: String(e)}));
            }
        });
    }
    """
    try:
        result = page.evaluate(js, {
            "url": url,
            "fields": fields,
            "hasImagem": has_imagem,
            "timeout": timeout_ms,
        })
        return result.get("status", 0), result.get("body", "")
    except Exception as e:
        logger.error("_browser_post exception: %s", e)
        return 0, str(e)


# ---------------------------------------------------------------------------
# Criação (API JSON e fallback PHP form)
# ---------------------------------------------------------------------------
def _try_json_api_raw(page, api_url: str, headers: dict, payload: dict, timeout_ms: int = REQUEST_TIMEOUT_MS):
    for attempt in range(1, CREATE_RETRIES + 1):
        try:
            response = page.request.post(api_url, data=json.dumps(payload), headers=headers, timeout=timeout_ms)
            return response
        except Exception:
            time.sleep(0.5 * attempt)
    return None


def _try_php_form(page, base_url: str, item: dict) -> tuple[bool, int, str]:
    type_map = {"text": "T", "select": "S", "textarea": "A"}
    tipo = type_map.get(item.get("type", "text"), "T")
    form_data = {
        "nome_loja": item.get("custom_name", ""),
        "nome_adm": item.get("name", ""),
        "ativa": item.get("active", "1"),
        "exibir_valor": item.get("display_value", "0"),
        "obrigatorio": item.get("required", "0"),
        "contador": item.get("max_length", "0"),
        "tipo": tipo,
        "valor": item.get("value", "0.00"),
        "add_total": item.get("add_total", "1"),
        "ordem": item.get("order", "0"),
    }
    php_url = f"{base_url}/admin/informacao_produto_executar.php?acao=incluir"
    status, body = _browser_post(page, php_url, form_data)
    return status in (200, 201, 302), status, body


# ---------------------------------------------------------------------------
# CREATE OPTION
# ---------------------------------------------------------------------------
def _create_option_via_php(page, base_url: str, field_id: int, option_data: dict) -> bool:
    url = (
        f"{base_url}/adm/extras/informacao_produto_index.php"
        f"?id={field_id}&aba=opcoes&acao=adicionar"
    )
    form_data = {
        "id": str(field_id),
        "id_opcao": "0",
        "exibicao_novo": "1",
        "opcao": option_data.get("value") or option_data.get("label", ""),
        "valor": option_data.get("price", "0.00"),
        "imagem": "",
    }
    status, body = _browser_post(page, url, form_data)
    success = status in (200, 201, 302)
    if not success:
        logger.error("Falha ao criar opção. Status: %s, Body: %s", status, body)
    return success


# ---------------------------------------------------------------------------
# EDIT OPTION (corrige nome/valor de uma opção existente)
# Mesmo endpoint do adicionar, mas com id_opcao = id real (não 0)
# ---------------------------------------------------------------------------
def _edit_option_via_php(page, base_url: str, field_id: int, option_id: int, option_data: dict) -> bool:
    url = (
        f"{base_url}/adm/extras/informacao_produto_index.php"
        f"?id={field_id}&aba=opcoes&acao=adicionar"
    )
    form_data = {
        "id": str(field_id),
        "id_opcao": str(option_id),
        "exibicao_novo": "1",
        "opcao": option_data.get("value") or option_data.get("label", ""),
        "valor": option_data.get("price", "0.00"),
        "imagem": "",
    }
    status, body = _browser_post(page, url, form_data)
    success = status in (200, 201, 302)
    if not success:
        logger.error("Falha ao editar opção %s. Status: %s, Body: %s", option_id, status, body)
    else:
        logger.debug("Edit opção %s status=%s body=%s", option_id, status, body)
        print(f" [resp:{status} body={repr(body[:120])}]", end="")
    return success


def _edit_option_via_ui_form(page, base_url: str, field_id: int, option_id: int, option_data: dict) -> bool:
    """
    Fallback robusto: abre a tela de edição da opção e submete o formulário real da página.
    Isso preserva todos os campos ocultos/tokens exigidos pelo backend Tray.
    """
    edit_url = (
        f"{base_url}/adm/extras/informacao_produto_index.php"
        f"?id={field_id}&aba=opcoes&acao=editar&id_opcao={option_id}"
    )
    try:
        page.goto(edit_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT_MS)
        page.wait_for_timeout(500)

        value = option_data.get("value") or option_data.get("label", "")
        price = option_data.get("price", "0.00")

        ok = page.evaluate(
            """
            ({value, price}) => {
                const forms = Array.from(document.querySelectorAll('form'));
                const form = forms.find(f => f.querySelector('[name="opcao"]')) || forms[0];
                if (!form) return false;

                const opcao = form.querySelector('[name="opcao"]');
                if (!opcao) return false;
                opcao.value = String(value ?? '');

                const valor = form.querySelector('[name="valor"]');
                if (valor) valor.value = String(price ?? '0.00');

                const submitBtn =
                    form.querySelector('button[type="submit"]') ||
                    form.querySelector('input[type="submit"]') ||
                    form.querySelector('[name="salvar"]') ||
                    form.querySelector('[name="acao"][value="salvar"]');

                if (submitBtn && typeof submitBtn.click === 'function') {
                    submitBtn.click();
                } else if (typeof form.requestSubmit === 'function') {
                    form.requestSubmit();
                } else {
                    form.submit();
                }
                return true;
            }
            """,
            {"value": value, "price": price},
        )

        if not ok:
            logger.error("Fallback UI edit não encontrou formulário/opcao para id_opcao=%s", option_id)
            return False

        page.wait_for_load_state("domcontentloaded", timeout=REQUEST_TIMEOUT_MS)
        page.wait_for_timeout(600)
        return True
    except Exception as e:
        logger.error("Erro no fallback UI edit id_opcao=%s: %s", option_id, e)
        return False


# ---------------------------------------------------------------------------
# DELETE OPTION
# ---------------------------------------------------------------------------
def _delete_option_via_php(page, base_url: str, field_id: int, option_id: int) -> bool:
    """
    POST para /adm/js/informacoes_adicionais.php via fetch() no browser.
    Cookies da sessão enviados automaticamente.
    """
    url = f"{base_url}/adm/js/informacoes_adicionais.php"
    form_data = {
        "acao": "excluir_opcao",
        "id_opcao": str(option_id),
        "imagem": "",
    }
    status, body = _browser_post(page, url, form_data)
    success = status in (200, 201, 302)
    if not success:
        logger.error("Falha ao deletar opção %s. Status: %s, Body: %s", option_id, status, body)
    return success
