

from dataclasses import dataclass, field
from hashlib import sha256


@dataclass(slots=True)
class ACLUser:
    """
    Represents a user in the ACL system.
    Attributes:
        name (str): The username.
        enabled (bool): Whether the user is enabled.
        nopass (bool): Whether the user has no password.
        passwords (set[bytes]): A set of hashed passwords.
    """
    name: str
    enabled: bool = True
    nopass: bool = False
    passwords: set[bytes] = field(default_factory=set)
    
    def check_password(self, password: bytes) -> bool:
        if not self.enabled:
            return False
        if self.nopass:
            return True
        hashed_password = sha256(password).digest()
        return hashed_password in self.passwords


@dataclass(slots=True)
class ClientSession:
    """
    Represents a client session for Redis connection management.
    This class manages the state of a connected client, including authentication,
    transaction state, and subscription information.
    Attributes:
        current_username (str | None): The username of the authenticated user, or None if not authenticated.
        in_multi (bool): Flag indicating whether the session is in transaction mode (MULTI).
        queued_commands (list[list[bytes]]): List of commands queued for execution in a transaction.
        in_subscribed_mode (bool): Flag indicating whether the session is in subscription mode.
        subscribed_channels (set[bytes]): Set of channel names the session is subscribed to.
    """
    
    current_username: str | None = None
    in_multi: bool = False
    queued_commands: list[list[bytes]] = field(default_factory=list)
    in_subscribed_mode: bool = False
    subscribed_channels: set[bytes] = field(default_factory=set)
    
    @property
    def is_authenticated(self) -> bool:
        return self.current_username is not None
    
    def login(self, username: str) -> None:
        self.current_username = username
        
    def logout(self) -> None:
        self.current_username = None
        self.in_multi = False
        self.queued_commands.clear()
        
    @classmethod
    def create(cls, default_user: ACLUser | None = None) -> "ClientSession":
        session = cls()
        if default_user and default_user.enabled and default_user.nopass:
            session.current_username = default_user.name
        return session