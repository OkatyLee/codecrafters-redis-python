from random import randint

from app.session import ACLUser


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

        self.acl_users: dict[str, ACLUser] = {
            "default": ACLUser("default", enabled=True, nopass=True)
        }


    def increment_master_repl_offset(self, payload_len: int) -> None:
        if payload_len > 0:
            self.master_repl_offset += payload_len

    def increment_replica_repl_offset(self, payload_len: int) -> None:
        if payload_len > 0:
            self.replica_repl_offset += payload_len

    def get_replica_offset(self) -> int:
        return self.replica_repl_offset
        
    def get_acl_user(self, username: str) -> ACLUser | None:
        return self.acl_users.get(username)

    def ensure_acl_user(self, username: str) -> ACLUser:
        user = self.acl_users.get(username)
        if user is None:
            user = ACLUser(username)
            self.acl_users[username] = user
        return user
