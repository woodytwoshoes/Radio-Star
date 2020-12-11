from models.Tasks.DataframeTasks import FindSimilar
from luigi import build


def main():
    pipeline = [FindSimilar()]
    build(
        pipeline,
        local_scheduler=True,
    )
