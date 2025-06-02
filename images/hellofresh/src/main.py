#!/usr/bin/env python

import logging

from log_handler import initialize_logger

from food_pipeline.food_difficulty_pipeline import FoodDifficultyPipeline

logger = logging.getLogger("main")

def getPipeline(name):
    if(name == "FoodDifficultyPipeline"):
        return FoodDifficultyPipeline()
    else:
        raise ValueError("Pipeline not defined")

if __name__ == '__main__':

    initialize_logger()

    logger.info('Application started')

    # We can pick pipeline name from arguments
    pipeline = getPipeline("FoodDifficultyPipeline")

    pipeline.run_pipeline()

    logger.info('Application done')

