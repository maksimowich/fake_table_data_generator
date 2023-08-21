import sys
from loguru import logger
from fake_data_generator.columns_generator import \
    Column, CategoricalColumn, ContinuousColumn, StringColumn, CurrentTimestampColumn
from fake_data_generator.sources_formats import \
    generate_fake_table, generate_table_profile, generate_table_from_profile

logger.remove(0)
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
