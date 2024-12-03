import asyncio
import json
import logging
import sys
from pynvim import attach
import websockets
import threading
import time
from typing import List, Set, Dict
import uuid
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('collab_editor.log')
    ]
)
logger = logging.getLogger(__name__)

class NvimHandler:
    def __init__(self, socket_path: str, callback):
        self.nvim = attach('socket', path=socket_path)
        self.callback = callback
        self.last_content = ""
        self.is_updating = False
        logger.info(f"NvimHandler initialized with socket: {socket_path}")
        
    def start(self):
        def check_changes():
            while True:
                try:
                    if not self.is_updating:
                        # Get current buffer content
                        current = '\n'.join(self.nvim.current.buffer[:])
                        
                        # If content changed, notify callback
                        if current != self.last_content:
                            logger.debug(f"Content changed: {current[:50]}...")
                            self.callback(current)
                            self.last_content = current
                            
                    time.sleep(0.1)  # Small delay to prevent CPU overuse
                except Exception as e:
                    logger.error(f"Error in Nvim handler: {e}")
                    time.sleep(0.1)
        
        thread = threading.Thread(target=check_changes, daemon=True)
        thread.start()
        logger.info("NvimHandler thread started")
    
    def update_buffer(self, content: str):
        try:
            self.is_updating = True
            current = '\n'.join(self.nvim.current.buffer[:])
            
            if current != content:
                logger.debug(f"Updating buffer with: {content[:50]}...")
                lines = content.split('\n')
                self.nvim.current.buffer[:] = lines
                self.last_content = content
                
        except Exception as e:
            logger.error(f"Error updating buffer: {e}")
        finally:
            self.is_updating = False

@dataclass
class Document:
    content: str = ""
    version: int = 0

class DocumentManager:
    def __init__(self):
        self.documents: Dict[str, Document] = {}
        self.lock = asyncio.Lock()
    
    async def update_document(self, doc_id: str, content: str) -> int:
        async with self.lock:
            if doc_id not in self.documents:
                self.documents[doc_id] = Document()
            
            doc = self.documents[doc_id]
            doc.content = content
            doc.version += 1
            return doc.version
    
    def get_document(self, doc_id: str) -> Document:
        return self.documents.get(doc_id, Document())

class CollabClient:
    def __init__(self, nvim_socket: str, websocket_url: str, doc_id: str = "default"):
        self.client_id = str(uuid.uuid4())
        self.doc_id = doc_id
        self.nvim_handler = NvimHandler(nvim_socket, self._on_local_change)
        self.websocket = None
        self.websocket_url = websocket_url
        logger.info(f"Client initialized: {self.client_id}")
        
    async def start(self):
        self.nvim_handler.start()
        while True:
            try:
                async with websockets.connect(self.websocket_url) as websocket:
                    self.websocket = websocket
                    logger.info(f"Connected to server: {self.client_id}")
                    
                    # Send initial join message
                    await self._send_message({
                        'type': 'join',
                        'doc_id': self.doc_id
                    })
                    
                    await self._handle_messages()
                    
            except Exception as e:
                logger.error(f"Connection error: {e}")
                self.websocket = None
                await asyncio.sleep(1)
    
    async def _send_message(self, data: dict):
        if self.websocket:
            data['client_id'] = self.client_id
            data['timestamp'] = time.time()
            try:
                await self.websocket.send(json.dumps(data))
            except Exception as e:
                logger.error(f"Error sending message: {e}")
    
    def _on_local_change(self, content: str):
        asyncio.run_coroutine_threadsafe(
            self._send_message({
                'type': 'update',
                'doc_id': self.doc_id,
                'content': content
            }),
            asyncio.get_event_loop()
        )
    
    async def _handle_messages(self):
        async for message in self.websocket:
            try:
                data = json.loads(message)
                
                if data['client_id'] != self.client_id:
                    if data['type'] == 'update':
                        self.nvim_handler.update_buffer(data['content'])
                    elif data['type'] == 'sync':
                        self.nvim_handler.update_buffer(data['content'])
                        
            except Exception as e:
                logger.error(f"Error handling message: {e}")

class CollabServer:
    def __init__(self, host: str = 'localhost', port: int = 8765):
        self.host = host
        self.port = port
        self.clients: Dict[str, Set[websockets.WebSocketServerProtocol]] = {}
        self.doc_manager = DocumentManager()
        logger.info(f"Server initialized on {host}:{port}")
        
    async def start(self):
        async with websockets.serve(self._handle_client, self.host, self.port):
            logger.info(f"Server running on ws://{self.host}:{self.port}")
            await asyncio.Future()
    
    async def _handle_client(self, websocket):
        client_info = {'doc_id': None}
        
        try:
            async for message in websocket:
                data = json.loads(message)
                client_id = data.get('client_id')
                message_type = data.get('type')
                
                if message_type == 'join':
                    doc_id = data['doc_id']
                    client_info['doc_id'] = doc_id
                    if doc_id not in self.clients:
                        self.clients[doc_id] = set()
                    self.clients[doc_id].add(websocket)
                    
                    # Send current document state
                    doc = self.doc_manager.get_document(doc_id)
                    await websocket.send(json.dumps({
                        'type': 'sync',
                        'content': doc.content,
                        'client_id': 'server'
                    }))
                    
                elif message_type == 'update':
                    doc_id = data['doc_id']
                    content = data['content']
                    await self.doc_manager.update_document(doc_id, content)
                    await self._broadcast(message, doc_id, exclude=websocket)
                    
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            doc_id = client_info['doc_id']
            if doc_id and doc_id in self.clients:
                self.clients[doc_id].remove(websocket)
    
    async def _broadcast(self, message: str, doc_id: str, exclude=None):
        if doc_id in self.clients:
            for client in self.clients[doc_id]:
                if client != exclude:
                    try:
                        await client.send(message)
                    except Exception as e:
                        logger.error(f"Error broadcasting: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Server: python script.py --server")
        print("  Client: python script.py <nvim-socket-path> [doc_id]")
        sys.exit(1)

    try:
        if sys.argv[1] == "--server":
            asyncio.run(CollabServer().start())
        else:
            nvim_socket = sys.argv[1]
            doc_id = sys.argv[2] if len(sys.argv) > 2 else "default"
            client = CollabClient(nvim_socket, "ws://localhost:8765", doc_id)
            asyncio.run(client.start())
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
