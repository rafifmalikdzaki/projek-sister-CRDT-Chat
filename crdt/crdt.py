import logging
from dataclasses import dataclass
from typing import Optional, List, Dict

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CRDTCharID:
    site_id: str
    seq: int


@dataclass
class CRDTChar:
    char_id: CRDTCharID
    char: str
    is_deleted: bool = False


class CRDTDocument:
    def __init__(self, site_id: str):
        self.site_id = site_id
        self.sequence: List[CRDTChar] = []
        self.local_seq = 0
        self.id_index_map: Dict[CRDTCharID, int] = {}

    def _next_id(self) -> CRDTCharID:
        self.local_seq += 1
        return CRDTCharID(site_id=self.site_id, seq=self.local_seq)

    def _compare_ids(self, id1: CRDTCharID, id2: CRDTCharID):
        if id1.site_id < id2.site_id:
            return -1
        elif id1.site_id > id2.site_id:
            return 1
        else:
            return (id1.seq > id2.seq) - (id1.seq < id2.seq)

    def _insert_char(self, char: CRDTChar):
        inserted = False
        for i, c in enumerate(self.sequence):
            if self._compare_ids(char.char_id, c.char_id) < 0:
                self.sequence.insert(i, char)
                inserted = True
                break
        if not inserted:
            self.sequence.append(char)
        self.id_index_map = {ch.char_id: i for i, ch in enumerate(self.sequence)}

    def get_text(self) -> str:
        visible_chars = [c.char for c in self.sequence if not c.is_deleted]
        return "".join(visible_chars)

    def insert_char_at_offset(self, offset: int, char: str) -> dict:
        new_id = self._next_id()
        new_char = CRDTChar(char_id=new_id, char=char)
        self._insert_char(new_char)
        logger.info(f"Inserted char '{char}' at offset {offset}")
        return {
            "type": "insert",
            "site_id": self.site_id,
            "char_id": [new_id.site_id, new_id.seq],
            "char": char,
        }

    def delete_char_at_offset(self, offset: int) -> Optional[dict]:
        visible_indices = [i for i, c in enumerate(self.sequence) if not c.is_deleted]
        if offset < 0 or offset >= len(visible_indices):
            return None
        seq_index = visible_indices[offset]

        target_char = self.sequence[seq_index]
        if target_char.is_deleted:
            return None
        target_char.is_deleted = True
        self.id_index_map = {ch.char_id: i for i, ch in enumerate(self.sequence)}
        logger.info(f"Deleted char '{target_char.char}' at offset {offset}")
        return {
            "type": "delete",
            "site_id": self.site_id,
            "char_id": [target_char.char_id.site_id, target_char.char_id.seq],
            "char": target_char.char,
        }

    def apply_remote_operation(self, op: dict):
        op_type = op["type"]
        logger.info(f"Applying remote operation: {op}")
        remote_id = CRDTCharID(site_id=op["char_id"][0], seq=op["char_id"][1])

        if op_type == "insert":
            if remote_id not in self.id_index_map:
                new_char = CRDTChar(char_id=remote_id, char=op["char"])
                self._insert_char(new_char)
        elif op_type == "delete":
            if remote_id in self.id_index_map:
                idx = self.id_index_map[remote_id]
                self.sequence[idx].is_deleted = True

