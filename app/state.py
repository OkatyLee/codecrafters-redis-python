

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
import logging

from app.config import PubSubConfig, ServerConfig
from app.metrics import set_connected_replicas
from app.storage import CacheStorage


@dataclass(slots=True)
class AppState:
    """
    Application state manager for a Redis-like server.
    Maintains server configuration, data storage, and replication state including
    replica connections, ports, acknowledgment offsets, and pub/sub subscriptions.
    Attributes:
        config (ServerConfig): Server configuration settings.
        storage (CacheStorage): In-memory cache storage for key-value data.
        logger (logging.Logger): Logger instance for application logging.
        replica_writers (list[asyncio.StreamWriter]): Stream writers for connected replicas.
        replicas_ports (list[int]): Ports of registered replica servers.
        replica_ack_offsets (dict[asyncio.StreamWriter, int]): Maps replicas to their acknowledgment offsets.
        pubsub (dict[bytes, set[asyncio.StreamWriter]]): Maps channel names to subscribed replica writers.
        bgsave_task (asyncio.Task | None): Task for the background save operation.
    """
    config: ServerConfig
    storage: CacheStorage
    logger: logging.Logger
    pubsub_config: PubSubConfig = field(default_factory=PubSubConfig)
    replica_writers: list[asyncio.StreamWriter] = field(default_factory=list)
    replicas_ports: list[int] = field(default_factory=list)
    replica_ack_offsets: dict[asyncio.StreamWriter, int] = field(default_factory=dict)
    pubsub: dict[bytes, set[asyncio.StreamWriter]] = field(default_factory=lambda: defaultdict(set))
    bgsave_task: asyncio.Future | None = None
    
    
    
    def register_replica(self, port: int | None, writer: asyncio.StreamWriter | None) -> None:
        if port is not None and port not in self.replicas_ports:
            self.replicas_ports.append(port)
        if writer is not None and writer not in self.replica_writers:
            self.replica_writers.append(writer)
        set_connected_replicas(len(self.replica_writers))
            
    def unregister_replica(self, writer: asyncio.StreamWriter) -> None:
        if writer in self.replica_writers:
            self.replica_writers.remove(writer)
        self.replica_ack_offsets.pop(writer, None)
        set_connected_replicas(len(self.replica_writers))
        
    def get_replicas(self) -> list[asyncio.StreamWriter]:
        return self.replica_writers
    
    def set_replica_ack_offset(self, writer: asyncio.StreamWriter | None, offset: int) -> None:
        if writer is None:
            return
        self.replica_ack_offsets[writer] = offset
