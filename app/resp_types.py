
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence


@dataclass(slots=True)
class BaseRESPType(ABC):
    
    @abstractmethod
    def encode(self) -> bytes:
        pass
    

@dataclass(slots=True)
class SimpleStringType(BaseRESPType):
    value: str
    def encode(self) -> bytes:
        return b"+" + self.value.encode() + b"\r\n"


@dataclass(slots=True)
class IntegerType(BaseRESPType):
    value: int
    def encode(self) -> bytes:
        return b":" + str(self.value).encode() + b"\r\n"


@dataclass(slots=True)
class BulkStringType(BaseRESPType):
    value: bytes
    def encode(self) -> bytes:
        return b"$" + str(len(self.value)).encode() + b"\r\n" + self.value + b"\r\n"


@dataclass(slots=True)
class ArrayType(BaseRESPType):
    value: Sequence[BaseRESPType]
    def encode(self) -> bytes:
        encoded_elements = [element.encode() for element in self.value]
        return b"*" + str(len(self.value)).encode() + b"\r\n" + b"".join(encoded_elements)


@dataclass(slots=True)
class SimpleErrorType(BaseRESPType):
    value: str
    def encode(self) -> bytes:
        return b"-" + self.value.encode() + b"\r\n"


@dataclass(slots=True)
class NullBulkStringType(BaseRESPType):
    def encode(self) -> bytes:
        return b"$-1\r\n"


@dataclass(slots=True)
class NullArrayType(BaseRESPType):
    def encode(self) -> bytes:
        return b"*-1\r\n"


@dataclass(slots=True)
class RawResponse(BaseRESPType):
    value: bytes
    def encode(self) -> bytes:
        return self.value
