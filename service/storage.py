# service/storage.py
import os
import json
import threading
import csv
import tempfile
import shutil
from typing import Iterable, List, Dict, Any

DEFAULT_DIR = "produtos"
DEFAULT_JSON = os.path.join(DEFAULT_DIR, "ProdutosOrigem.json")
DEFAULT_CSV = os.path.join(DEFAULT_DIR, "ProdutosOrigem.csv")

class JSONStorage:
    def __init__(
        self,
        json_path: str = DEFAULT_JSON,
        csv_path: str = DEFAULT_CSV,
        replace_on_start: bool = False
    ):
        self.json_path = json_path
        self.csv_path = csv_path
        self.replace_on_start = replace_on_start

        os.makedirs(os.path.dirname(self.json_path) or ".", exist_ok=True)
        self._lock = threading.Lock()

        if self.replace_on_start:
            # inicia com lista vazia e grava imediatamente (destino)
            self._items: List[Dict[str, Any]] = []
            try:
                self._atomic_write_json(self._items)
            except Exception:
                # se falhar, ainda inicializa em memória
                pass
        else:
            # carrega o que já existe (origem)
            self._items: List[Dict[str, Any]] = self._load_existing()

    # ---- internal helpers ----
    def _load_existing(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.json_path):
            return []
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            # se arquivo estiver corrompido ou inválido, ignora e sobrescreve depois
            return []
        return []

    def _atomic_write_json(self, data: List[Dict[str, Any]]):
        """
        Escreve o arquivo inteiro de forma atômica (temp -> move).
        Isso garante que o arquivo final sempre seja um JSON completo.
        """
        dirpath = os.path.dirname(self.json_path) or "."
        fd, tmp = tempfile.mkstemp(prefix="tmp_products_", dir=dirpath)
        os.close(fd)
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            # rename atomically
            shutil.move(tmp, self.json_path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass

    # ---- public API ----
    def save(self, obj: Dict[str, Any]) -> None:
        """
        Salva um produto imediatamente no JSON (arquivo contém um array).
        Mantém o comportamento original: armazena objeto em memória e regrava o
        arquivo inteiro de forma atômica.
        """
        if not isinstance(obj, dict):
            return
        with self._lock:
            item = obj.copy()
            if "produto_id" in item:
                item["produto_id"] = str(item["produto_id"])
            self._items.append(item)
            try:
                # escreve o arquivo inteiro (não escreve em pedaços)
                self._atomic_write_json(self._items)
            except Exception as e:
                print(f"[storage] Erro ao salvar JSON: {e}")

    def save_many(self, objs: Iterable[Dict[str, Any]]) -> None:
        """
        Salva vários objetos de uma vez (usa mesma lógica atômica).
        Regrava o arquivo inteiro ao final da operação.
        ✅ MODIFICADO: Agora também exporta CSV automaticamente
        """
        with self._lock:
            added = 0
            for o in objs:
                if isinstance(o, dict):
                    if "produto_id" in o:
                        o["produto_id"] = str(o["produto_id"])
                    self._items.append(o.copy())
                    added += 1
            if added:
                try:
                    # escreve o arquivo inteiro (não escreve em pedaços)
                    self._atomic_write_json(self._items)
                    # ✅ NOVO: exporta CSV automaticamente após salvar JSON
                    self._export_csv_internal()
                except Exception as e:
                    print(f"[storage] Erro ao salvar many JSON: {e}")

    def _export_csv_internal(self):
        """
        ✅ NOVO: Método interno para exportar CSV (sem lock, chamado de dentro do save_many)
        """
        try:
            path = self.csv_path
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

            header = [
                "produto_id", "nome", "preco", "estoque", "estoque_minimo",
                "categoria", "referencia", "peso", "altura", "largura", "comprimento",
                "imagem_url", "notificacao_estoque_baixo", "itens_inclusos",
                "mensagem_adicional", "tempo_garantia", "seo_link", "seo_title",
                "seo_description", "descricao"
            ]

            dirpath = os.path.dirname(path) or "."
            fd, tmp = tempfile.mkstemp(prefix="tmp_products_csv_", dir=dirpath, text=True)
            os.close(fd)
            try:
                with open(tmp, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=header, extrasaction="ignore")
                    writer.writeheader()
                    for p in self._items:
                        row = {k: "" for k in header}
                        for k in ["produto_id", "nome", "preco", "estoque", "estoque_minimo",
                                  "categoria", "referencia", "peso", "altura", "largura", "comprimento",
                                  "imagem_url", "notificacao_estoque_baixo", "itens_inclusos",
                                  "mensagem_adicional", "tempo_garantia", "descricao"]:
                            if k in p:
                                row[k] = p.get(k, "")
                        seo = p.get("seo_preview") or {}
                        row["seo_link"] = seo.get("link") or ""
                        row["seo_title"] = seo.get("title") or ""
                        row["seo_description"] = seo.get("description") or ""
                        writer.writerow(row)
                shutil.move(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
        except Exception as e:
            print(f"[storage] Erro ao exportar CSV automaticamente: {e}")

    def read_all(self) -> List[Dict[str, Any]]:
        """Retorna a lista atual em memória (carregada do arquivo no startup)."""
        with self._lock:
            return list(self._items)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna um dicionário com estatísticas básicas:
         • total
         • com_preco
         • com_estoque
         • top_5_categorias (lista de tuplas (categoria, count))
        """
        with self._lock:
            total = len(self._items)
            com_preco = sum(1 for p in self._items if p.get("preco") not in (None, "", 0))
            com_estoque = sum(1 for p in self._items if p.get("estoque") not in (None, "", 0))
            categorias = {}
            for p in self._items:
                cat = p.get("categoria") or "Sem Categoria"
                categorias[cat] = categorias.get(cat, 0) + 1
            top_5 = sorted(categorias.items(), key=lambda x: x[1], reverse=True)[:5]
            return {
                "total": total,
                "com_preco": com_preco,
                "com_estoque": com_estoque,
                "top_5_categorias": top_5
            }

    def export_csv(self, path: str | None = None) -> str:
        """
        Exporta o conteúdo atual para CSV. Retorna o caminho do arquivo gerado.
        Se path for None, usa self.csv_path.
        """
        path = path or self.csv_path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        # define cabeçalho consistente
        header = [
            "produto_id", "nome", "preco", "estoque", "estoque_minimo",
            "categoria", "referencia", "peso", "altura", "largura", "comprimento",
            "imagem_url", "notificacao_estoque_baixo", "itens_inclusos",
            "mensagem_adicional", "tempo_garantia", "seo_link", "seo_title",
            "seo_description", "descricao"
        ]

        with self._lock:
            dirpath = os.path.dirname(path) or "."
            fd, tmp = tempfile.mkstemp(prefix="tmp_products_csv_", dir=dirpath, text=True)
            os.close(fd)
            try:
                with open(tmp, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=header, extrasaction="ignore")
                    writer.writeheader()
                    for p in self._items:
                        row = {k: "" for k in header}
                        for k in ["produto_id", "nome", "preco", "estoque", "estoque_minimo",
                                  "categoria", "referencia", "peso", "altura", "largura", "comprimento",
                                  "imagem_url", "notificacao_estoque_baixo", "itens_inclusos",
                                  "mensagem_adicional", "tempo_garantia", "descricao"]:
                            if k in p:
                                row[k] = p.get(k, "")
                        seo = p.get("seo_preview") or {}
                        row["seo_link"] = seo.get("link") or ""
                        row["seo_title"] = seo.get("title") or ""
                        row["seo_description"] = seo.get("description") or ""
                        writer.writerow(row)
                shutil.move(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
        return path

    def clear(self):
        """Limpa memória e arquivo (útil para testes)."""
        with self._lock:
            self._items = []
            try:
                if os.path.exists(self.json_path):
                    os.remove(self.json_path)
            except Exception:
                pass


# INSTÂNCIAS ISOLADAS (origem e destino).
# origem: mantém histórico (append)
# destino: recria arquivo no início da execução (replace_on_start=True)
storage_origem = JSONStorage(
    json_path=os.path.join(DEFAULT_DIR, "ProdutosOrigem.json"),
    csv_path=os.path.join(DEFAULT_DIR, "ProdutosOrigem.csv"),
    replace_on_start=False
)

storage_destino = JSONStorage(
    json_path=os.path.join(DEFAULT_DIR, "ProdutosDestino.json"),
    csv_path=os.path.join(DEFAULT_DIR, "ProdutosDestino.csv"),
    replace_on_start=True
)

# compatibilidade com import existing code: `from service.storage import storage`
storage = storage_origem