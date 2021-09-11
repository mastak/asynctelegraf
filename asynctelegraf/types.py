from typing import List, Dict, Union, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from asynctelegraf.client import TelegrafClient
else:
    TelegrafClient = TypeVar('TelegrafClient')

TagsType = Union[List[str], Dict[str, str]]
