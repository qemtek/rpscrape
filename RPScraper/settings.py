import boto3
import pandas as pd

from RPScraper.src.utils.config import get_attribute

PROJECT_DIR = get_attribute('PROJECT_DIR')
S3_BUCKET = get_attribute('S3_BUCKET')

AWS_GLUE_DB = get_attribute('AWS_GLUE_DB')
AWS_GLUE_TABLE = get_attribute('AWS_GLUE_TABLE')

AWS_ACCESS_KEY_ID = get_attribute('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_attribute('AWS_SECRET_ACCESS_KEY')

boto3_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

SCHEMA_COLUMNS = {
    'id': 'int',
    'date': 'timestamp',
    'course': 'string',
    'off': 'string',
    # 'name': 'string',
    'type': 'string',
    'class': 'string',
    'pattern': 'string',
    'rating_band': 'string',
    'age_band': 'string',
    'sex_rest': 'string',
    'dist_m': 'int',
    'going': 'string',
    'num': 'int',
    'pos': 'string',
    'ran': 'int',
    'draw': 'int',
    'btn': 'double',
    'ovr_btn': 'double',
    'horse': 'string',
    # 'sp': 'string',
    'dec': 'double',
    'age': 'int',
    'sex': 'string',
    'lbs': 'int',
    'hg': 'string',
    'time': 'double',
    'jockey': 'string',
    'trainer': 'string',
    'or': 'double',
    'rpr': 'double',
    'ts': 'double',
    'prize': 'string',
    'sire': 'string',
    'dam': 'string',
    'damsire': 'string',
    'owner': 'string',
    'comment': 'string',
    'country': 'string',
    "course_id": 'int',
    'race_name': 'string',
    "race_id": 'int',
    "horse_id": 'int',
    "jockey_id": 'int',
    "trainer_id": 'int',
    "owner_id": 'int',
    "dam_id": 'int',
    "damsire_id": 'int',
    "silk_url": 'string',
    'horse_cleaned': 'string',
    'jockey_cleaned': 'string',
    'trainer_cleaned': 'string',
    'year': 'int'
}


def get_dtype(string):
    if string == 'int':
        return int
    elif string == 'double':
        return float
    elif string == 'string':
        return str
    elif string == 'timestamp':
        return str


COL_DTYPES = dict()
for key, value in SCHEMA_COLUMNS.items():
    COL_DTYPES[key] = get_dtype(value)


OUTPUT_COLS = ['id', 'date', 'course', 'off', 'type', 'class', 'pattern', 'rating_band', 'age_band',
 'sex_rest', 'dist_m', 'going', 'num', 'pos', 'ran', 'draw', 'btn', 'ovr_btn', 'horse',
 'dec', 'age', 'sex', 'lbs', 'hg', 'time', 'jockey', 'trainer', 'or', 'rpr', 'ts', 'prize',
 'sire', 'dam', 'damsire', 'owner', 'comment', 'country', 'course_id', 'race_id', 'horse_id',
 'jockey_id', 'trainer_id', 'owner_id', 'dam_id', 'damsire_id', 'silk_url', 'horse_cleaned',
 'jockey_cleaned', 'trainer_cleaned', 'year']

