from configs.settings import settings
import logger.logger as logger
from jobs.process_pipeline_tresvista import ProcessTresvista

def run():
    """
    This is the driver function of the package which calls
    the Tresvista Process Pipeline method to process data.
    """
    logger.initialize_logger("Tresvista_log")
    settings.logger.info("Logger initialized!")
    settings.logger.info("Initializing Process Pipeline...")
    pt = ProcessTresvista()
    pt.process_src_dataset()
    settings.logger.info("Tresvista Process Pipeline completed successfully!!!")

if __name__ == '__main__':
    run()