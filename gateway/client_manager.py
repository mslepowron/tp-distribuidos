from threading import Lock
from typing import Dict, Optional
from client_handler import ClientHandler

import logging
logger = logging.getLogger("ClientManager")

class ClientManager:
    def __init__(self):
        self._clients: Dict[str, ClientHandler] = {}
        self._lock = Lock()

    def add(self, client: ClientHandler):
        with self._lock:
            self._clients[client.get_client_id()] = client

    def remove(self, client: ClientHandler):
        with self._lock:
            client_id = client.get_client_id()
            if client_id in self._clients:
                del self._clients[client_id]

    def get_by_uuid(self, uuid_str: str) -> Optional[ClientHandler]:
        with self._lock:
            return self._clients.get(uuid_str)

    def get_all(self) -> Dict[str, ClientHandler]:
        with self._lock:
            return dict(self._clients)  # shallow copy

    def count(self) -> int:
        with self._lock:
            return len(self._clients)

    def clear(self):
        with self._lock:
            for client in self._clients.values():
                try:
                    client._stop_client()
                except Exception as e:
                    logger.warning(f"Error stopping client: {e}")
            self._clients.clear()
