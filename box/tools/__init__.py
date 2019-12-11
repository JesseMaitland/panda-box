import pandas as pd
from typing import List
from box.exceptions import NoPandaError, PandaExistsError


class Panda:

    def __init__(self, name: str, data: pd.DataFrame):
        self.name = name
        self.data = data


class PandaBox:
    """ The Panda Box - Keep those unruly Pandas in order

    This object is used to manage a group of data frames as returned by the other stuff.... TODO: make comment
    """

    def __init__(self, *pandas):
        """
        :param root_path: Path to a collection of .sql query files
        """
        self.pandas = list(pandas)

    def __iter__(self):
        return PandaIterator(self)

    @property
    def panda_names(self) -> List[str]:
        """
        :return: List of panda names in this PandaBox object
        """
        return [panda.name for panda in self.pandas]

    def get_panda(self, name: str) -> Panda:
        """
        Returns a copy of the panda from the PandaBox. If not found a NoPandaError is raised.
        :param name: name of desired panda
        :return: Panda object
        """
        for panda in self.pandas:
            if panda.name == name:
                return panda
        raise NoPandaError(f"no panda found with name {name}")

    def remove_panda(self, name: str) -> Panda:
        """
        Returns a Panda object, removing it from the collection in the PandaBox. If not found a no panda error is raised.
        :param name: name of desired panda
        :return: Panda object
        """
        for i in range(len(self.pandas)):
            if name == self.pandas[i].name:
                delattr(self, self.pandas[i].name)
                return self.pandas.pop(i)
        raise NoPandaError(f"no panda found with name {name}")

    def add_panda(self, panda: Panda) -> None:
        """
        Adds a panda into the PandaBox. If a panda with the same name is found, a PandaExistsError is raised.
        :param panda: the panda to add into the PandaBox
        :return: None
        """
        if panda.name in self.panda_names:
            raise PandaExistsError
        else:
            self.pandas.append(panda)
            setattr(self, panda.name, panda)

    def destroy_panda(self, name: str) -> None:
        """
        Destroys a panda object by calling the del function. Allows
        :param name:
        :return:
        """
        for i in range(len(self.pandas)):
            if name == self.pandas[i].name:
                panda = self.pandas.pop(i)
                delattr(self, panda.name)
                del panda
                return
        raise NoPandaError(f"no panda found with name {name}")

    def update_panda(self, panda: Panda):
        for i in range(len(self.pandas) - 1):
            if panda.name == self.pandas[i].name:
                self.pandas[i] = panda
                return
        raise NoPandaError(f"no panda found with name {panda.name}")


class PandaIterator:
    """
    Simple iterator class for the PandaBox Object
    """

    def __init__(self, panda_box: PandaBox):
        self._index = 0
        self._panda_box = panda_box

    def __next__(self):
        if self._index > len(self._panda_box.pandas) - 1:
            raise StopIteration
        else:
            result = self._panda_box.pandas[self._index]
            self._index += 1
        return result

