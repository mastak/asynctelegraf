from typing import TYPE_CHECKING, Dict, List, TypeVar, Union

if TYPE_CHECKING:
    from asynctelegraf.client import TelegrafClient
else:
    TelegrafClient = TypeVar('TelegrafClient')

TagsType = Union[List[str], Dict[str, str], None]
