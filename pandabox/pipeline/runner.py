from pandabox.pipeline.step import FetchDbDataStep, TransformDataStep
from pandabox.tools import PandaBox
from typing import List


class PipelineRunner:
    """
    Class is used to group and execute a series of transformation steps into a simple pipeline.

    Fetch Data steps are preformed first, and will pass the returned data automatically to the next
    step in the pipeline.

    The transformation steps are then executed, passing their output automatically to the next step.
    The final result is returned in the form of a PandaBox object.

    """
    def __init__(self,
                 transform_steps: List[TransformDataStep],
                 fetch_data_steps: List[FetchDbDataStep] = None,
                 panda_box: PandaBox = None
                 ):
        """
        Args:
            transform_steps: List of steps to executed against the data returned from the fetch steps
            fetch_data_steps: List of steps to call to fetch data into the pipeline
            panda_box: Optional panda box object, who's data will be added into the pipeine
        """

        if panda_box is None and fetch_data_steps is None:
            raise Exception("either panda_box, or fetch_data_steps must be provided")

        if not fetch_data_steps:
            fetch_data_steps = []

        if not panda_box:
            panda_box = PandaBox()

        self.fetch_data_steps = fetch_data_steps
        self.transform_steps = transform_steps
        self.panda_box = panda_box

    def run(self) -> PandaBox:
        """
        Call this method to run all of the steps which have been added to the pipeline

        Returns: PandaBox containing the data as determined by the transformation and fetch steps
        """
        data_box = PandaBox()

        for fetch_data_step in self.fetch_data_steps:
            step = fetch_data_step()
            panda_box = step.execute()

            for panda in panda_box:
                data_box.add_panda(panda)

        for transform_step in self.transform_steps:
            step = transform_step(data_box)
            data_box = step.execute()

        return data_box
