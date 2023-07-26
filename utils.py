from typing import Callable, Any


class AttrObj:
    def __init__(self, obj):
        self.__obj = obj

    @property
    def _first(self):
        return self[0]

    def filter(self, func: Callable[[Any, Any], Any]):
        def _filter(iterator):
            return AttrObj([v for k, v in iterator if func(k, AttrObj(v))])
        if isinstance(self.__obj, dict):
            return _filter(self.__obj.items)
        elif isinstance(self.__obj, list):
            return _filter(enumerate(self.__obj))
        return AttrObj(None)

    def filter_one(self, func: Callable[[Any, Any], Any]):
        return self.filter(func)[0]

    def __getattr__(self, name):
        if isinstance(self.__obj, dict):
            return AttrObj(self.__obj.get(name))
        return AttrObj(None)

    def __getitem__(self, index):
        if isinstance(self.__obj, list):
            try:
                return AttrObj(self.__obj[index])
            except IndexError:
                return AttrObj(None)
        return AttrObj(None)

    def __str__(self):
        return str(self.__obj)

    def __repr__(self):
        return f'<AttrObj {self.__obj}>'

    def __bool__(self):
        return bool(self.__obj)

    def __eq__(self, other):
        return self.__obj == other

    @property
    def value(self):
        return self.__obj
