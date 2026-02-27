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
    def __init__(self, json_path: str = DEFAULT_JSON, csv_path: str = DEFAULT_CSV):
        self.json_path = json_path
        self.csv_path = csv_path
        os.makedirs(os.path.dirname(self.json_path) or ".", exist_ok=True)
        self._lock = threading.Lock()
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
        Também adiciona na memória para estatísticas.
        """
        if not isinstance(obj, dict):
            return
        with self._lock:
            # normalizar/limpar campos mínimos
            item = obj.copy()
            # força string id se existir
            if "produto_id" in item:
                item["produto_id"] = str(item["produto_id"])
            self._items.append(item)
            # escreve o arquivo inteiro (suficiente para até milhares de registros)
            try:
                self._atomic_write_json(self._items)
            except Exception as e:
                # se falhar, mantém em memória (não propaga para não quebrar o scraper)
                print(f"[storage] Erro ao salvar JSON: {e}")

    def save_many(self, objs: Iterable[Dict[str, Any]]) -> None:
        """
        Salva vários objetos de uma vez (usa mesma lógica atômica).
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
                    self._atomic_write_json(self._items)
                except Exception as e:
                    print(f"[storage] Erro ao salvar many JSON: {e}")

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
            # escreve em temp e move
            dirpath = os.path.dirname(path) or "."
            fd, tmp = tempfile.mkstemp(prefix="tmp_products_csv_", dir=dirpath, text=True)
            os.close(fd)
            try:
                with open(tmp, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=header, extrasaction="ignore")
                    writer.writeheader()
                    for p in self._items:
                        row = {k: "" for k in header}
                        # mapeia campos simples
                        for k in ["produto_id", "nome", "preco", "estoque", "estoque_minimo",
                                  "categoria", "referencia", "peso", "altura", "largura", "comprimento",
                                  "imagem_url", "notificacao_estoque_baixo", "itens_inclusos",
                                  "mensagem_adicional", "tempo_garantia", "descricao"]:
                            if k in p:
                                row[k] = p.get(k, "")
                        # metadados SEO
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


# expose default instance for `from service import storage`
storage = JSONStorage()