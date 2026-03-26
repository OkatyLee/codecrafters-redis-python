from hashlib import sha256

from app.commands import Arity, CommandContext, NullArray, command
from app.parser import RESPError
from app.storage import get_storage


@command(
    name=b"PING",
    arity=Arity(0, 1),
    flags={"readonly", "fast"},
    allowed_in_subscribe=True
)
def cmd_ping(ctx: CommandContext, args: list[bytes]) -> str | bytes | list[bytes]:
    if ctx.session.in_subscribed_mode:
        payload = args[0] if args else b""
        return [b"pong", payload]

    if args:
        return args[0]
    return "PONG"


@command(
    name=b"ECHO",
    arity=Arity(1, 1),
    flags={"readonly", "fast"},
    allowed_in_subscribe=True

)
def cmd_echo(ctx: CommandContext, args: list[bytes]) -> bytes:
    return args[0]


@command(
    name=b"AUTH",
    arity=Arity(1, 2),
    flags={"connection", "fast"},
    allowed_before_auth=True,
    allowed_in_subscribe=True,
)
def cmd_auth(ctx: CommandContext, args: list[bytes]) -> str:
    config = ctx.config
    session = ctx.session
                
    username, password = (args[0], args[1]) if len(args) == 2 else (b"default", args[0])
    user = config.get_acl_user(username.decode())
    if user is None or not user.check_password(password):
        raise ValueError("WRONGPASS invalid username-password pair or user is disabled.")

    ctx.session.login(user.name)
    return "OK"


def _acl_setuser(ctx: CommandContext, username: bytes, password: bytes) -> str:
        
        if not password.startswith(b'>'):
            raise RESPError("invalid password format for 'ACL SETUSER'")

        user = ctx.config.ensure_acl_user(username.decode())
        hashed_password = sha256(password[1:]).digest()
        user.passwords.add(hashed_password)
        user.nopass = False
        return "OK"


def _acl_getuser(ctx: CommandContext, username: bytes) -> list[bytes | list[bytes]] | NullArray:
    user = ctx.config.get_acl_user(username.decode())
    if user is None:
        return NullArray()
    
    return [
        b"flags",
        [b"nopass"] if user.nopass else [],
        b"password",
        [p for p in user.passwords] if len(user.passwords) > 0 else []

    ]



@command(
    name=b"ACL",
    arity=Arity(1, None),
    flags={"admin"},
)
def cmd_acl(ctx: CommandContext, args: list[bytes]) -> str | list[bytes | list[bytes]] | NullArray | bytes:
    match args[0].upper():
        
        case b"WHOAMI":
            username = ctx.session.current_username if ctx.session.current_username else "default" 
            return username.encode()
            
        case b"GETUSER":
            if len(args) != 2:
                raise RESPError("wrong number of arguments for 'ACL GETUSER'")
            return _acl_getuser(ctx, args[1])

        case b"SETUSER":
            if len(args) != 3:
                raise RESPError("wrong number of arguments for 'ACL SETUSER'")
            return _acl_setuser(ctx, args[1], args[2])
        
        case _:
            raise RESPError("ERR unknown subcommand or wrong number of arguments for 'ACL'")


