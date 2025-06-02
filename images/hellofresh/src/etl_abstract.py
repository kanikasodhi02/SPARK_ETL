from pyspark.sql import SparkSession
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger("ETLAbstract")

class ETLAbstract(ABC):

    def __init__(self, name):
        self.spark = SparkSession.builder.appName(name)\
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
            .getOrCreate()
        self.name = name


    def run_pipeline(self):
        try:
            logger.info(f'{self.name} pipeline started')

            logger.info(f'{self.name} extraction started')
            extract_data = self.extract()
            logger.info(f'{self.name} extraction done')

            logger.info(f'{self.name} transform started')
            transform_data = self.transform(extract_data)
            logger.info(f'{self.name} transform done')

            logger.info(f'{self.name} loading started')
            self.load(transform_data)
            logger.info(f'{self.name} loading done')

            logger.info(f'{self.name} pipeline done')
        except Exception as error:
            logger.error(f'{self.name} FoundError {error}', exc_info=True)

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        pass
