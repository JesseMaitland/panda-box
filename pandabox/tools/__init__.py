import pandas as pd
from typing import List
from pandabox.exceptions import NoPandaError, PandaExistsError


class Panda:

    """
    Simple data class which allows reference to a named data frame within a collection.
    """

    def __init__(self, name: str, data: pd.DataFrame):
        """
        Args:
            name: used to reference this `Panda` from within the `PandaBox`
            data: data frame
        """
        self.name = name
        self.data = data


class PandaBox:
    """ The Panda Box - Keep those unruly Pandas in order

    This object is used to manage a group of data frames which can be accessed using a `name`
    attribute. The intent of this object is to serve as an organisational service for multiple
    pandas data frames.
    """

    def __init__(self, *pandas):
        """
        Args:
            *pandas: list of `Panda` objects to add to the `PandaBox`
        """
        self.pandas = list(pandas)

    def __iter__(self):
        return PandaIterator(self)

    def __len__(self):
        return len(self.pandas)

    @property
    def panda_names(self) -> List[str]:
        """
        List of available `Panda` names

        Returns:
            List[str]
        """
        return [panda.name for panda in self.pandas]

    def get_panda(self, name: str) -> Panda:
        """
        Fetches a `Panda` from the `PandaBox`. The returned object will still be linked to the `PandaBox`
        and will be mutable
        Args:
            name: the desired `Panda` to return

        Returns:
            Panda
        """
        for panda in self.pandas:
            if panda.name == name:
                return panda
        raise NoPandaError(f"no panda found with name {name}")

    def remove_panda(self, name: str) -> Panda:
        """
        Removes a desired `Panda` from the `PandaBox` if the name is not found, a `NoPandaError`
        exception is raised.

        Args:
            name: the 'Panda' to remove from the box

        Returns:
            Panda
        """
        for i in range(len(self.pandas)):
            if name == self.pandas[i].name:
                return self.pandas.pop(i)
        raise NoPandaError(f"no panda found with name {name}")

    def add_panda(self, panda: Panda) -> None:
        """
        Adds a `Panda` into the `PandaBox.
        If a `Panda` with the same name is found, a `PandaExistsError` is raised.

        Args:
            panda: the `Panda` to add to the `PandaBox`

        Returns:
            None
        """
        if panda.name in self.panda_names:
            raise PandaExistsError
        else:
            self.pandas.append(panda)
            setattr(self, panda.name, panda)

    def destroy_panda(self, name: str) -> None:
        """
        Destroys a `Panda` by using the `del` function.
        If the panda does not exits a `NoPandaError` is raised

        Args:
            name: name of the `Panda` to remove

        Returns:
            None
        """
        for i in range(len(self.pandas)):
            if name == self.pandas[i].name:
                panda = self.pandas.pop(i)  # noqa
                del(panda)
                return
        raise NoPandaError(f"no panda found with name {name}")

    def update_panda(self, panda: Panda) -> None:
        """
        Updates a `Panda` object which already exists in the `PandaBox. If the `Panda` can not be found
        a `NoPandaError` exception is raised.

        Args:
            panda: The desired `Panda` to update

        Returns:
            None
        """
        for i in range(len(self.pandas) - 1):
            if panda.name == self.pandas[i].name:
                self.pandas[i] = panda
                return
        raise NoPandaError(f"no panda found with name {panda.name}")


class PandaIterator:
    """
    Iterator class used by the `PandaBox` object
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
