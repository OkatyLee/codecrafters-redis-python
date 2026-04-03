from hashlib import sha256

from app.commands import Arity, CommandContext, command
from app.parser import RESPError
from app.resp_types import ArrayType, BaseRESPType, BulkStringType, NullArrayType, SimpleStringType


@command(
    name=b"PING",
    arity=Arity(0, 1),
    flags={"readonly", "fast"},
    allowed_in_subscribe=True
)
def cmd_ping(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    if ctx.session.in_subscribed_mode:
        payload = args[0] if args else b""
        resp_response = [BulkStringType(b"pong"), BulkStringType(payload)]
        return ArrayType(resp_response)

    if args:
        return BulkStringType(args[0])
    return SimpleStringType("PONG")


@command(
    name=b"ECHO",
    arity=Arity(1, 1),
    flags={"readonly", "fast"},
)
def cmd_echo(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    return BulkStringType(args[0])


@command(
    name=b"AUTH",
    arity=Arity(1, 2),
    flags={"connection", "fast"},
    allowed_before_auth=True,
)
def cmd_auth(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    session = ctx.session         
    username, password = (args[0], args[1]) if len(args) == 2 else (b"default", args[0])
    user = config.get_acl_user(username.decode())
    if user is None or not user.check_password(password):
        raise RESPError("WRONGPASS invalid username-password pair or user is disabled.")

    ctx.session.login(user.name)
    return SimpleStringType("OK")


def _acl_setuser(ctx: CommandContext, username: bytes, password: bytes) -> str:
        if not password.startswith(b'>'):
            raise RESPError("ERR invalid password format for 'ACL SETUSER'")

        user = ctx.app_state.config.ensure_acl_user(username.decode())
        hashed_password = sha256(password[1:]).digest()
        user.passwords.add(hashed_password)
        user.nopass = False
        return "OK"


def _acl_getuser(ctx: CommandContext, username: bytes) -> list[bytes | list[bytes]] | None:
    user = ctx.app_state.config.get_acl_user(username.decode())
    if user is None:
        return None
    
    return [
        b"flags",
        [b"nopass"] if user.nopass else [],
        b"passwords",
        [p for p in user.passwords] if len(user.passwords) > 0 else []

    ]



@command(
    name=b"ACL",
    arity=Arity(1, None),
    flags={"admin"},
)
def cmd_acl(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    match args[0].upper():
        
        case b"WHOAMI":
            username = ctx.session.current_username if ctx.session.current_username else "default" 
            return BulkStringType(username.encode())
            
        case b"GETUSER":
            if len(args) != 2:
                raise RESPError("ERR wrong number of arguments for 'ACL GETUSER'")
            result = _acl_getuser(ctx, args[1])
            if result is None:
                return NullArrayType()
            resp_result = []
            for item in result:
                if isinstance(item, bytes):
                    resp_result.append(BulkStringType(item))
                elif isinstance(item, list):
                    resp_result.append(
                        ArrayType(
                        [BulkStringType(subitem) for subitem in item]
                        )
                    )

            return ArrayType(resp_result)

        case b"SETUSER":
            if len(args) != 3:
                raise RESPError("ERR wrong number of arguments for 'ACL SETUSER'")
            return SimpleStringType(_acl_setuser(ctx, args[1], args[2]))
        
        case _:
            raise RESPError("ERR unknown subcommand or wrong number of arguments for 'ACL'")


