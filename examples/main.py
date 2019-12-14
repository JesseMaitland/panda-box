from pandabox.tools import PandaBox
from pandabox.db.dbmanager import DbManager
from pandabox.db.queries import DbQueries
from pandabox.pipeline.step import FetchDbDataStep, TransformDataStep
from pandabox.pipeline.runner import PipelineRunner
from pathlib import Path


class Transform(TransformDataStep):

    def __init__(self, panda_box: PandaBox):
        super().__init__(panda_box)

    def execute(self) -> PandaBox:
        self.add_column_to_tables()
        return self.panda_box

    def add_column_to_tables(self):
        panda = self.panda_box.remove_panda("tables")

        panda.data["new_col"] = "cool value"

        self.panda_box.add_panda(panda)


dbm = DbManager()
db_conn = dbm.get_alchemy_engine("staging_connection")

query_path = Path("/Users/jessemaitland/PycharmProjects/pandabox/sample_queries")
db_queries = DbQueries(query_path, db_conn)

fetch_data = FetchDbDataStep(db_queries)


pipeline = PipelineRunner(fetch_data_steps=[fetch_data],
                          transform_steps=[Transform])

panda_box = pipeline.run()

for panda in panda_box:
    print(panda.data.info())
