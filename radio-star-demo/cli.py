from .models.Tasks.DataframeTasks import FindSimilar, PullSimilarImages
from luigi import build


def main():
    pipeline = [FindSimilar(), PullSimilarImages()]
    build(
        pipeline,
        local_scheduler=True,
    )
