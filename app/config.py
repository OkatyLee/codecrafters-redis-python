import asyncio
from collections import defaultdict
from random import randint


class ServerConfig:
    def __init__(self, host: str, port: int, replicaof: str | None = None, dir: str = "tmp/files", dbfilename: str = "dump.rdb"):
        self.host = host
        self.port = port
        self.replicaof = replicaof
        self.role = "master" if replicaof is None else "slave"
        self.dir = dir
        self.dbfilename = dbfilename
        abc = '1234567890abcdefghijklmnopqrstuvwxyz'
        self.master_perlid = ''.join(abc[randint(0, len(abc) - 1)] for _ in range(40))
        self.master_repl_offset = 0
        self.replica_repl_offset = 0
        self.replicas_ports: list[int] = []
        self.replica_writers: list[asyncio.StreamWriter] = []
        self.replica_ack_offsets: dict[asyncio.StreamWriter, int] = {}
        # channel -> set of subscribed writers
        self.pubsub: defaultdict[bytes, set[asyncio.StreamWriter]] = defaultdict(set)

    def register_replica(self, port: int | None, writer: asyncio.StreamWriter | None) -> None:
        if port is not None and port not in self.replicas_ports:
            self.replicas_ports.append(port)
        if writer is not None and writer not in self.replica_writers:
            self.replica_writers.append(writer)

    def unregister_replica(self, writer: asyncio.StreamWriter) -> None:
        if writer in self.replica_writers:
            self.replica_writers.remove(writer)
        self.replica_ack_offsets.pop(writer, None)

    def increment_master_repl_offset(self, payload_len: int) -> None:
        if payload_len > 0:
            self.master_repl_offset += payload_len

    def increment_replica_repl_offset(self, payload_len: int) -> None:
        if payload_len > 0:
            self.replica_repl_offset += payload_len
            
    def get_replicas(self) -> list[asyncio.StreamWriter]:
        return self.replica_writers

    def get_replica_offset(self) -> int:
        return self.replica_repl_offset

    def set_replica_ack_offset(self, writer: asyncio.StreamWriter | None, offset: int) -> None:
        if writer is None:
            return
        self.replica_ack_offsets[writer] = offset
        
