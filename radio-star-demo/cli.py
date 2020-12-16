from .models.Tasks.DataframeTasks import FindSimilar, PullSimilarImages
from luigi import build
import argparse

parser = argparse.ArgumentParser(description="Parameters for radio-star Tasks")

parser.add_argument(
    "-f",
    "--fractional_search",
    type=float,
    default=0.2,
    help="Determines the fraction "
    "of the "
    "total dataset to load in order to find images with similar features."
    "WARNING: accepts values "
    "between 0 and "
    "1. Values close to 1 may exceed computer memory limit",
)

parser.add_argument(
    "-i",
    "--index_of_comparator",
    type=int,
    default=78414,
    help="The index of the image we are seeking to interpet. All other images found will be similar to this comparator image. Default value is chosen in order to fetch a chest x ray with interesting and clear features.",
)


def main(args=None):
    args = parser.parse_args(args=args)
    pipeline = [
        FindSimilar(
            comparator_index=args.index_of_comparator,
            fractional_search=args.fractional_search,
        ),
        PullSimilarImages(),
    ]
    build(
        pipeline,
        local_scheduler=True,
    )
