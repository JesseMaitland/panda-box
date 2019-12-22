from abc import ABC, abstractmethod
from pandabox.tools import PandaBox
from pandabox.db.queries import DbQueries


class Step(ABC):
    """
    Abstract Base Class used to subclass a custom step
    """
    @abstractmethod
    def execute(self) -> PandaBox:
        pass


class FetchDbDataStep(Step):
    """
    Base Class used to subclass a fetch data step.
    """
    def __init__(self, queries: DbQueries):
        """
        Args:
            queries: DbQueries which will be executed in this step
        """
        self.queries = queries

    def execute(self) -> PandaBox:
        """
        Default execute method, can be overridden, however must return a PandaBox.

        Returns: PandaBox
        """
        panda_box = self.queries.fetch_all()
        return panda_box


class TransformDataStep(Step):
    """
    Abstract Base class used to subclass a data transformation step
    """
    def __init__(self, panda_box: PandaBox):
        """
        Args:
            panda_box: The data source on which the transformation will be preformed
        """
        self.panda_box = panda_box

    @abstractmethod
    def execute(self) -> PandaBox:
        """
        Must be implemented in the child class and return a PandaBox.

        Returns: PandaBox
        """
        pass
