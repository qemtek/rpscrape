import boto3
import os

PROJECT_DIR =  os.path.dirname(os.path.abspath(__file__))

# S3 Configuration
S3_BUCKET = 'rpscrape'  # For raw data
S3_GLUE_PATH = "s3://rpscrape/datasets"  # Keep existing Glue table path

# AWS Glue Configuration
AWS_GLUE_DB = 'finish-time-predict'  # Use existing database
AWS_RPSCRAPE_TABLE_NAME = 'rpscrape'

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# When running in AWS, boto3 will automatically use the task role credentials
boto3_session = boto3.Session(
    region_name='eu-west-1',
    profile_name=os.environ.get('AWS_PROFILE', None)
)

SCHEMA_COLUMNS = {
    'id': 'int',
    'date': 'timestamp',
    'course': 'string',
    'off': 'string',
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
    'draw': 'double',  # Changed from int to double since values are floats
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
    'secs': 'double',
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
    "sire_id": 'int',
    "damsire_id": 'int',
    "silk_url": 'string',
    "year": 'int',
    'horse_cleaned': 'string',
    'created_at': 'timestamp'
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


OUTPUT_COLS = ['id', 'date', 'course', 'off', 'race_name', 'type', 'class', 'pattern', 'rating_band', 'age_band',
 'sex_rest', 'dist_m', 'going', 'num', 'pos', 'ran', 'draw', 'btn', 'ovr_btn', 'horse',
 'dec', 'age', 'sex', 'lbs', 'hg', 'time', 'jockey', 'trainer', 'or', 'rpr', 'ts', 'prize',
 'sire', 'dam', 'damsire', 'owner', 'comment', 'country', 'course_id', 'race_id', 'horse_id',
 'jockey_id', 'trainer_id', 'owner_id', 'dam_id', 'sire_id', 'damsire_id', 'silk_url', 'horse_cleaned', 'year', 'created_at']
