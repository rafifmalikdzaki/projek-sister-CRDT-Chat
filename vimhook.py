import asyncio
import json
import websockets
import logging
import sys
from pynvim import attach
from dataclasses import dataclass
from typing import List, Optional
import uuid
import random
from websockets.exceptions import ConnectionClosed
from contextlib import asynccontextmanager
import threading
from concurrent.futures import ThreadPoolExecutor

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

@dataclass
class CRDTOperation:
    site_id: str
    lamport_timestamp: int
    character: str
    position: List[int]
    line_number: int
    is_delete: bool = False


class NvimEventHandler:
    def __init__(self, nvim, callback):
        self.nvim = nvim
        self.callback = callback
        self._running = True
        self._thread = None

    def start(self):
        """Start the Neovim event handler."""
        self._thread = threading.Thread(target=self._run_event_loop)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        """Stop the Neovim event handler."""
        self._running = False
        if self._thread:
            self._thread.join()

    def _run_event_loop(self):
        """Run the Neovim event loop to process changes."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self._running:
            try:
                event = self.nvim.next_message()
                logger.debug(f"Neovim event: {event}")
                if event and event[0] == "lines":
                    asyncio.run_coroutine_threadsafe(
                        self.callback(event[1]), loop
                    )
            except Exception as e:
                logger.error(f"Error in Nvim event loop: {e}")
                break

class WebSocketServer:
    def __init__(self, host: str = 'localhost', port: int = 8765):
        self.host = host
        self.port = port
        self.clients = set()

    async def start(self):
        async def handler(websocket):
            self.clients.add(websocket)
            logger.info(f"New client connected. Total clients: {len(self.clients)}")

            try:
                async for message in websocket:
                    await self._broadcast(message, exclude=websocket)
            except ConnectionClosed:
                logger.info("Client connection closed")
            finally:
                self.clients.remove(websocket)

        async with websockets.serve(handler, self.host, self.port):
            logger.info(f"WebSocket server running on ws://{self.host}:{self.port}")
            await asyncio.Future()  # Run forever

    async def _broadcast(self, message: str, exclude):
        logger.debug(f"Broadcasting message: {message}")
        for client in self.clients:
            if client != exclude:
                try:
                    await client.send(message)
                except ConnectionClosed:
                    logger.warning("Failed to send message to client")


class CollaborativeEditor:
    def __init__(self, nvim_socket_path: str, websocket_url: str):
        self.nvim = attach('socket', path=nvim_socket_path)
        self.websocket_url = websocket_url
        self.site_id = str(uuid.uuid4())
        self.websocket = None
        self.document = CRDTDocument(self.site_id)
        self.nvim_handler = NvimEventHandler(self.nvim, self._handle_nvim_change)

    async def _handle_nvim_change(self, args):
        logger.info(f"Neovim change detected: {args}")
        try:
            buffnr, first_line, last_line, *_ = args
            changes = self.nvim.call('nvim_buf_get_lines', buffnr, first_line, last_line, False)
            logger.debug(f"Buffer changes: {changes}")
            operations = self._process_changes(first_line, changes)
            for op in operations:
                logger.debug(f"Generated operation: {op}")
                await self._send_operation(op)
        except Exception as e:
            logger.error(f"Error handling nvim change: {e}")

    async def _handle_websocket_events(self):
        async for message in self.websocket:
            logger.info(f"Received WebSocket message: {message}")
            try:
                operation = json.loads(message)
                self._apply_remote_operation(operation)
            except Exception as e:
                logger.error(f"Error handling websocket message: {e}")

    async def _send_operation(self, operation: CRDTOperation):
        try:
            message = json.dumps(operation.__dict__)
            logger.info(f"Sending operation: {message}")
            await self.websocket.send(message)
        except Exception as e:
            logger.error(f"Error sending operation: {e}")

    async def connect(self):
        while True:
            try:
                async with websockets.connect(self.websocket_url) as websocket:
                    self.websocket = websocket
                    logger.info("Connected to WebSocket server")
                    self.nvim_handler.start()
                    await self._handle_websocket_events()
            except Exception as e:
                logger.warning(f"Connection error: {e}. Retrying...")
                await asyncio.sleep(1)

    async def start(self):
        await self.connect()

    def _process_changes(self, first_line: int, changes: List[str]) -> List[CRDTOperation]:
        logger.debug(f"Processing changes from line {first_line}: {changes}")
        operations = []
        for line_num, line in enumerate(changes, start=first_line):
            old_line = self.document.get_text()[line_num] if line_num < len(self.document.lines) else ""
            logger.debug(f"Old line: {old_line}, New line: {line}")
            matcher = SequenceMatcher(None, old_line, line)
            for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                logger.debug(f"Diff tag: {tag}, i1: {i1}, i2: {i2}, j1: {j1}, j2: {j2}")
                if tag == "insert":
                    for idx, char in enumerate(line[j1:j2]):
                        operations.append(self.document.insert(char, line_num, j1 + idx))
                elif tag == "delete":
                    for idx in range(i1, i2):
                        operations.append(self.document.delete(line_num, idx))
                elif tag == "replace":
                    for idx in range(i1, i2):
                        operations.append(self.document.delete(line_num, idx))
                    for idx, char in enumerate(line[j1:j2]):
                        operations.append(self.document.insert(char, line_num, j1 + idx))
        logger.debug(f"Generated operations: {operations}")
        return operations

class CRDTDocument:
    def __init__(self, site_id: str):
        self.site_id = site_id
        self.lines = []
        self.lamport_clock = 0

    def insert(self, char: str, line_num: int, index: int) -> CRDTOperation:
        """Insert a character into the document."""
        self.lamport_clock += 1
        while len(self.lines) <= line_num:
            self.lines.append([])
        position = [random.randint(1, 100)]  # Simplified position
        operation = CRDTOperation(
            site_id=self.site_id,
            lamport_timestamp=self.lamport_clock,
            character=char,
            position=position,
            line_number=line_num,
        )
        self.lines[line_num].insert(index, (char, position))
        logger.info(f"Insert: '{char}' at line {line_num}, index {index} with position {position}")
        return operation

    def delete(self, line_num: int, index: int) -> Optional[CRDTOperation]:
        """Delete a character from the document."""
        if line_num < len(self.lines) and index < len(self.lines[line_num]):
            self.lamport_clock += 1
            char, position = self.lines[line_num].pop(index)
            operation = CRDTOperation(
                site_id=self.site_id,
                lamport_timestamp=self.lamport_clock,
                character=char,
                position=position,
                line_number=line_num,
                is_delete=True,
            )
            logger.info(f"Delete: '{char}' from line {line_num}, index {index} with position {position}")
            return operation
        logger.warning(f"Delete failed: line {line_num}, index {index} does not exist.")
        return None

    def _apply_operation(self, operation: CRDTOperation):
        """Apply a CRDT operation."""
        if operation.is_delete:
            logger.info(f"Applying delete: '{operation.character}' at line {operation.line_number}")
            self.delete(operation.line_number, operation.position[-1])
        else:
            logger.info(f"Applying insert: '{operation.character}' at line {operation.line_number}, position {operation.position}")
            self.insert(
                operation.character, 
                operation.line_number, 
                operation.position[-1] if operation.position else 0
            )

    def get_text(self) -> List[str]:
        """Get the current text of the document."""
        return ["".join(char for char, _ in line) for line in self.lines]

def main():
    if "--server" in sys.argv:
        asyncio.run(WebSocketServer().start())
    elif len(sys.argv) > 1:
        nvim_socket_path = sys.argv[1]
        editor = CollaborativeEditor(nvim_socket_path, websocket_url="ws://localhost:8765")
        asyncio.run(editor.start())
    else:
        print("Usage: python collab.py --server OR python collab.py <nvim-socket-path>")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

