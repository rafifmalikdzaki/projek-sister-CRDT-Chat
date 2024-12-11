import asyncio
import json
import websockets
import logging
import sys
from pynvim import attach
from dataclasses import dataclass
from typing import Optional, List, Dict
import uuid
import threading
from websockets.exceptions import ConnectionClosed
import os

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collaborative_editor.log"),
        logging.StreamHandler()
    ]
)
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
    """
    A simplified CRDT document (RGA-like).
    Characters are sorted by (site_id lex order, seq numeric order).
    Each inserted character gets a unique ID.
    Deletions mark characters as deleted.
    """

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
        return {
            "type": "insert",
            "site_id": self.site_id,
            "char_id": [new_id.site_id, new_id.seq],
            "char": char
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
        return {
            "type": "delete",
            "site_id": self.site_id,
            "char_id": [target_char.char_id.site_id, target_char.char_id.seq],
            "char": target_char.char
        }

    def apply_remote_operation(self, op: dict):
        op_type = op["type"]
        logger.info(f"Applying remote operation: {op_type}")
        remote_id = CRDTCharID(site_id=op["char_id"][0], seq=op["char_id"][1])

        if op_type == "insert":
            if remote_id not in self.id_index_map:
                new_char = CRDTChar(char_id=remote_id, char=op["char"])
                self._insert_char(new_char)
        elif op_type == "delete":
            if remote_id in self.id_index_map:
                idx = self.id_index_map[remote_id]
                self.sequence[idx].is_deleted = True


class NvimEventHandler:
    def __init__(self, nvim, callback, event_loop, channel_id):
        self.nvim = nvim
        self.callback = callback
        self._running = True
        self._thread = None
        self._last_content = ""
        self.event_loop = event_loop
        self.channel_id = channel_id

    def start(self):
        self._thread = threading.Thread(target=self._run_event_loop)
        self._thread.daemon = True
        self._thread.start()
        logger.info("NvimEventHandler thread started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
        logger.info("NvimEventHandler stopped")

    def _run_event_loop(self):
        try:
            # Use channel_id for rpcnotify and echom for debugging
            self.nvim.command(f'''
                let g:channel_id = {self.channel_id}
                function! CollabEditNotify()
                    echom "CollabEditNotify called"
                    let bufnr = bufnr('%')
                    let changes = nvim_buf_get_lines(bufnr, 0, -1, v:false)
                    call rpcnotify(g:channel_id, 'buffer_changed', bufnr, changes)
                    return ""
                endfunction
            ''')

            self.nvim.command('augroup CollabEdit')
            self.nvim.command('autocmd!')
            self.nvim.command('autocmd TextChanged,TextChangedI * call CollabEditNotify()')
            self.nvim.command('augroup END')

            self._last_content = "\n".join(self.nvim.current.buffer[:])
            logger.info("Neovim handler initialized with initial content")

            while self._running:
                try:
                    event = self.nvim.next_message()
                    if event:
                        logger.info(f"Received event from Nvim: {event}")
                        event_type, name, args = event
                        if event_type == 'notification' and name == 'buffer_changed':
                            new_content = "\n".join(args[1])
                            if new_content != self._last_content:
                                future = asyncio.run_coroutine_threadsafe(
                                    self.callback(args),
                                    self.event_loop
                                )
                                self._last_content = new_content
                except Exception as e:
                    logger.error(f"Error in Nvim event loop: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Fatal error in Nvim handler: {e}", exc_info=True)


class WebSocketServer:
    def __init__(self, host: str = 'localhost', port: int = 8765):
        self.host = host
        self.port = port
        self.clients = set()

    async def start(self):
        async def handler(websocket):
            self.clients.add(websocket)
            client_id = str(id(websocket))[-6:]
            logger.info(f"New client {client_id} connected. Total clients: {len(self.clients)}")

            try:
                async for message in websocket:
                    await self._broadcast(message, exclude=websocket)
            except ConnectionClosed:
                logger.info(f"Client {client_id} connection closed")
            finally:
                self.clients.remove(websocket)
                logger.info(f"Client {client_id} removed. Total clients: {len(self.clients)}")

        async with websockets.serve(handler, self.host, self.port):
            logger.info(f"WebSocket server running on ws://{self.host}:{self.port}")
            await asyncio.Future()  # run forever

    async def _broadcast(self, message: str, exclude):
        for client in self.clients:
            if client != exclude:
                try:
                    await client.send(message)
                except ConnectionClosed:
                    logger.warning("Failed to send message to a client")


class CollaborativeEditor:
    def __init__(self, nvim_socket_path: str, websocket_url: str):
        self.loop = asyncio.get_event_loop()
        logger.info(f"Initializing CollaborativeEditor with socket path: {nvim_socket_path}")
        try:
            self.nvim = attach('socket', path=nvim_socket_path)
            logger.info("Successfully attached to Neovim")
        except Exception as e:
            logger.error(f"Failed to attach to Neovim: {e}", exc_info=True)
            raise

        self.websocket_url = websocket_url
        self.site_id = str(uuid.uuid4())
        logger.info(f"Editor site_id: {self.site_id}")

        # Get channel_id using the Neovim API
        self.channel_id, _ = self.nvim.api.get_api_info()
        logger.info(f"Using channel_id {self.channel_id} for RPC notifications")

        self.websocket = None
        self.document = CRDTDocument(self.site_id)
        initial_text = "\n".join(self.nvim.current.buffer[:])
        for ch in initial_text:
            self.document.insert_char_at_offset(len(self.document.sequence), ch)
        self.nvim_handler = None
        self.last_known_text = initial_text
        logger.info(f"Initial text: {self.last_known_text}")

    async def _handle_nvim_change(self, args):
        try:
            new_lines = args[1]
            new_text = "\n".join(new_lines)
            old_text = self.last_known_text

            logger.info(f"Old text: {old_text}")
            logger.info(f"New text: {new_text}")

            # Compute longest common prefix
            prefix_len = 0
            while prefix_len < len(old_text) and prefix_len < len(new_text) and old_text[prefix_len] == new_text[prefix_len]:
                prefix_len += 1

            # Compute longest common suffix
            suffix_len = 0
            while suffix_len < (len(old_text) - prefix_len) and suffix_len < (len(new_text) - prefix_len) and \
                  old_text[-(suffix_len+1)] == new_text[-(suffix_len+1)]:
                suffix_len += 1

            old_mid = old_text[prefix_len:len(old_text)-suffix_len]
            new_mid = new_text[prefix_len:len(new_text)-suffix_len]

            logger.info(f"Old middle: {old_mid}")
            logger.info(f"New middle: {new_mid}")

            operations = []
            # Delete old_mid
            for i in range(len(old_mid)):
                op = self.document.delete_char_at_offset(prefix_len)
                if op:
                    operations.append(op)

            # Insert new_mid
            for i, ch in enumerate(new_mid):
                op = self.document.insert_char_at_offset(prefix_len + i, ch)
                operations.append(op)

            logger.info(f"Generated {len(operations)} operations from Neovim change")

            # Send operations to WebSocket
            for op in operations:
                if self.websocket:
                    logger.info(f"Sending operation: {op}")
                    await self._send_operation(op)
                else:
                    logger.warning("WebSocket not connected, cannot send operation")

            self.last_known_text = new_text
        except Exception as e:
            logger.error(f"Error handling Neovim change: {e}", exc_info=True)

    async def _send_operation(self, operation: dict):
        try:
            message = json.dumps(operation)
            await self.websocket.send(message)
        except Exception as e:
            logger.error(f"Error sending operation: {e}", exc_info=True)

    async def _handle_websocket_events(self):
        async for message in self.websocket:
            logger.info(f"Received message from WebSocket: {message}")
            try:
                operation_data = json.loads(message)
                await self._apply_remote_operation(operation_data)
            except Exception as e:
                logger.error(f"Error handling websocket message: {e}", exc_info=True)

    async def _apply_remote_operation(self, operation_data: dict):
        try:
            if operation_data.get("site_id") == self.site_id:
                return
            self.document.apply_remote_operation(operation_data)
            updated_text = self.document.get_text()
            if updated_text != self.last_known_text:
                self.nvim.current.buffer[:] = updated_text.split('\n')
                self.last_known_text = updated_text
        except Exception as e:
            logger.error(f"Error applying remote operation: {e}", exc_info=True)

    async def connect(self):
        while True:
            try:
                logger.info(f"Attempting to connect to {self.websocket_url}")
                async with websockets.connect(self.websocket_url) as websocket:
                    self.websocket = websocket
                    logger.info("Successfully connected to WebSocket server")

                    if not self.nvim_handler:
                        logger.info("Initializing NvimEventHandler")
                        self.nvim_handler = NvimEventHandler(self.nvim, self._handle_nvim_change, self.loop, self.channel_id)
                        self.nvim_handler.start()

                    await self._handle_websocket_events()
            except Exception as e:
                logger.error(f"Connection error: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def start(self):
        logger.info("Starting CollaborativeEditor")
        await self.connect()


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Server mode: python collab.py --server")
        print("  Client mode: python collab.py <nvim-socket-path>")
        sys.exit(1)

    try:
        if sys.argv[1] == "--server":
            asyncio.run(WebSocketServer().start())
        else:
            nvim_socket_path = sys.argv[1]
            editor = CollaborativeEditor(nvim_socket_path, "ws://localhost:8765")
            asyncio.run(editor.start())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

