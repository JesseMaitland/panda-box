from pandabox.pipeline.step import FetchDbDataStep, TransformDataStep
from pandabox.tools import PandaBox
from typing import List


class PipelineRunner:

    def __init__(self, fetch_data_steps: List[FetchDbDataStep], transform_steps: List[TransformDataStep]):
        self.fetch_data_steps = fetch_data_steps
        self.transform_steps = transform_steps

    def run(self):
        data_box = PandaBox()

        for fetch_data_step in self.fetch_data_steps:
            panda_box = fetch_data_step.execute()

            for panda in panda_box:
                data_box.add_panda(panda)

        for transform_step in self.transform_steps:
            step = transform_step(data_box)
            data_box = step.execute()

        return data_box
