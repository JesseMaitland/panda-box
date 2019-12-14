from abc import ABC, abstractmethod
from pandabox.tools import PandaBox
from pandabox.db.queries import DbQueries


class Step(ABC):

    @abstractmethod
    def execute(self) -> PandaBox:
        pass


class FetchDbDataStep(Step):

    def __init__(self, queries: DbQueries):
        self.queries = queries

    def execute(self) -> PandaBox:
        panda_box = self.queries.fetch_all()
        return panda_box


class TransformDataStep(Step):

    def __init__(self, panda_box: PandaBox):
        self.panda_box = panda_box

    def execute(self) -> PandaBox:
        pass
