# service/storage.py
import os
import json
import threading
import csv
import tempfile
import shutil
from typing import Iterable, List, Dict, Any

DEFAULT_DIR = "produtos"

class JSONStorage:
    def __init__(
        self,
        json_path: str,
        csv_path: str,
        replace_on_start: bool = False
    ):
        self.json_path = json_path
        self.csv_path = csv_path
        self.replace_on_start = replace_on_start

        os.makedirs(os.path.dirname(self.json_path) or ".", exist_ok=True)
        self._lock = threading.Lock()

        if self.replace_on_start:
            self._items: List[Dict[str, Any]] = []
            self._atomic_write_json(self._items)   # limpa só se explicitamente pedido
        else:
            self._items: List[Dict[str, Any]] = self._load_existing()

    def _load_existing(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.json_path):
            return []
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception:
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
            shutil.move(tmp, self.json_path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except:
                    pass

    def save(self, obj: Dict[str, Any]) -> None:
        if not isinstance(obj, dict):
            return
        with self._lock:
            item = obj.copy()
            if "produto_id" in item:
                item["produto_id"] = str(item["produto_id"])
            self._items.append(item)
            self._atomic_write_json(self._items)

    def save_many(self, objs: Iterable[Dict[str, Any]]) -> None:
        with self._lock:
            for o in objs:
                if isinstance(o, dict):
                    item = o.copy()
                    if "produto_id" in item:
                        item["produto_id"] = str(item["produto_id"])
                    self._items.append(item)
            if self._items:
                self._atomic_write_json(self._items)
                self._export_csv_internal()

    def _export_csv_internal(self):
        # (mesmo código de antes - mantido igual)
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
                    try: os.remove(tmp)
                    except: pass
        except Exception as e:
            print(f"[storage] Erro CSV: {e}")

    def read_all(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._items)

    def clear(self):
        """Limpa memória e arquivo (usado apenas na opção 2)"""
        with self._lock:
            self._items = []
            try:
                if os.path.exists(self.json_path):
                    os.remove(self.json_path)
                # recria arquivo vazio para não dar erro em outras partes
                self._atomic_write_json([])
            except Exception:
                pass

    # demais métodos (get_statistics, export_csv) permanecem iguais...


# ==================== INSTÂNCIAS GLOBAIS (compatibilidade) ====================
storage_origem = JSONStorage(
    json_path=os.path.join(DEFAULT_DIR, "ProdutosOrigem.json"),
    csv_path=os.path.join(DEFAULT_DIR, "ProdutosOrigem.csv"),
    replace_on_start=False
)

storage_destino = JSONStorage(   # ← agora também False por segurança
    json_path=os.path.join(DEFAULT_DIR, "ProdutosDestino.json"),
    csv_path=os.path.join(DEFAULT_DIR, "ProdutosDestino.csv"),
    replace_on_start=False
)

storage = storage_origem