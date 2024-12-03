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
from difflib import SequenceMatcher
import threading
from itertools import zip_longest

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
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
        self._loop = None
        self._last_content = ""

    def start(self):
        self._thread = threading.Thread(target=self._run_event_loop)
        self._thread.daemon = True
        self._thread.start()
        logger.debug("NvimEventHandler thread started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
        logger.debug("NvimEventHandler stopped")

    def _run_event_loop(self):
        try:
            # Set up Vim function for buffer changes
            self.nvim.command('''
                function! CollabEditNotify()
                    let bufnr = bufnr('%')
                    let changes = nvim_buf_get_lines(bufnr, 0, -1, v:false)
                    call rpcnotify(0, 'buffer_changed', bufnr, changes)
                    return ""
                endfunction
            ''')

            # Register autocommands for all types of changes
            self.nvim.command('augroup CollabEdit')
            self.nvim.command('autocmd!')
            self.nvim.command('autocmd TextChanged,TextChangedI * call CollabEditNotify()')
            self.nvim.command('augroup END')

            # Store initial content
            self._last_content = "\n".join(self.nvim.current.buffer[:])
            logger.info("Neovim handler initialized with initial content")

            while self._running:
                try:
                    event = self.nvim.next_message()
                    if event:
                        name, args = event
                        if name == 'buffer_changed':
                            logger.debug(f"Buffer change event received: {args}")
                            new_content = "\n".join(args[1])
                            if new_content != self._last_content:
                                asyncio.run_coroutine_threadsafe(
                                    self.callback(args),
                                    asyncio.get_event_loop()
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
                    logger.debug(f"Server received message from client {client_id}: {message}")
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
        logger.debug(f"Broadcasting message to {len(self.clients)-1} clients: {message}")
        for client in self.clients:
            if client != exclude:
                try:
                    await client.send(message)
                except ConnectionClosed:
                    logger.warning(f"Failed to send message to client {str(id(client))[-6:]}")

class CRDTDocument:
    def __init__(self, site_id: str):
        self.site_id = site_id
        self.lines = [[]]  # List of lines, each containing (char, position) tuples
        self.lamport_clock = 0

    def insert(self, char: str, line_num: int, index: int) -> CRDTOperation:
        self.lamport_clock += 1
        
        # Ensure line exists
        while len(self.lines) <= line_num:
            self.lines.append([])
            
        # Generate position identifier
        position = self._generate_position(line_num, index)
        
        # Create operation
        operation = CRDTOperation(
            site_id=self.site_id,
            lamport_timestamp=self.lamport_clock,
            character=char,
            position=position,
            line_number=line_num
        )
        
        self._apply_operation(operation)
        return operation

    def delete(self, line_num: int, index: int) -> Optional[CRDTOperation]:
        if line_num >= len(self.lines) or index >= len(self.lines[line_num]):
            return None
            
        self.lamport_clock += 1
        char, position = self.lines[line_num][index]
        
        operation = CRDTOperation(
            site_id=self.site_id,
            lamport_timestamp=self.lamport_clock,
            character=char,
            position=position,
            line_number=line_num,
            is_delete=True
        )
        
        self._apply_operation(operation)
        return operation

    def _generate_position(self, line_num: int, index: int) -> List[int]:
        line = self.lines[line_num]
        
        if len(line) == 0:
            return [random.randint(1, 1000)]
            
        if index >= len(line):
            last_pos = line[-1][1][0]
            return [last_pos + random.randint(1, 100)]
            
        if index == 0:
            first_pos = line[0][1][0]
            return [max(1, first_pos // 2)]
            
        left_pos = line[index - 1][1][0]
        right_pos = line[index][1][0]
        middle = (left_pos + right_pos) // 2
        
        if middle == left_pos:
            middle = left_pos + 1
            
        return [middle]

    def _apply_operation(self, operation: CRDTOperation):
        line_num = operation.line_number
        
        while len(self.lines) <= line_num:
            self.lines.append([])
            
        if operation.is_delete:
            self.lines[line_num] = [(c, p) for c, p in self.lines[line_num] 
                                  if p != operation.position]
        else:
            # Insert character at correct position
            insert_idx = 0
            for idx, (_, pos) in enumerate(self.lines[line_num]):
                if pos > operation.position:
                    break
                insert_idx = idx + 1
                
            self.lines[line_num].insert(insert_idx, (operation.character, operation.position))

    def get_text(self) -> List[str]:
        return [''.join(char for char, _ in line) for line in self.lines]

class CollaborativeEditor:
    def __init__(self, nvim_socket_path: str, websocket_url: str):
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
        self.websocket = None
        self.document = CRDTDocument(self.site_id)
        self.nvim_handler = None

    async def _handle_nvim_change(self, args):
        try:
            buffnr, lines = args
            current_text = self.document.get_text()
            operations = []
            
            # Process changes
            for line_num, (old_line, new_line) in enumerate(zip_longest(current_text, lines, fillvalue="")):
                logger.debug(f"Processing line {line_num}: old='{old_line}' new='{new_line}'")
                matcher = SequenceMatcher(None, old_line, new_line)
                
                for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                    if tag == "insert":
                        for idx, char in enumerate(new_line[j1:j2]):
                            op = self.document.insert(char, line_num, j1 + idx)
                            operations.append(op)
                    elif tag == "delete":
                        for idx in range(i1, i2):
                            op = self.document.delete(line_num, idx)
                            if op:
                                operations.append(op)
                    elif tag == "replace":
                        # Handle as delete + insert
                        for idx in range(i1, i2):
                            op = self.document.delete(line_num, idx)
                            if op:
                                operations.append(op)
                        for idx, char in enumerate(new_line[j1:j2]):
                            op = self.document.insert(char, line_num, j1 + idx)
                            operations.append(op)

            # Send operations
            for op in operations:
                if self.websocket:
                    await self._send_operation(op)
                else:
                    logger.warning("WebSocket not connected, cannot send operation")
                    
        except Exception as e:
            logger.error(f"Error handling Neovim change: {e}", exc_info=True)

    async def _send_operation(self, operation: CRDTOperation):
        try:
            message = json.dumps(operation.__dict__)
            logger.debug(f"Sending operation: {message}")
            await self.websocket.send(message)
        except Exception as e:
            logger.error(f"Error sending operation: {e}", exc_info=True)

    async def _handle_websocket_events(self):
        async for message in self.websocket:
            try:
                operation_data = json.loads(message)
                await self._apply_remote_operation(operation_data)
            except Exception as e:
                logger.error(f"Error handling websocket message: {e}", exc_info=True)

    async def _apply_remote_operation(self, operation_data: dict):
        try:
            operation = CRDTOperation(**operation_data)
            if operation.site_id != self.site_id:  # Only apply remote operations
                self.document._apply_operation(operation)
                text = self.document.get_text()
                self.nvim.current.buffer[:] = text
                logger.debug(f"Applied remote operation: {operation}")
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
                        self.nvim_handler = NvimEventHandler(self.nvim, self._handle_nvim_change)
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
