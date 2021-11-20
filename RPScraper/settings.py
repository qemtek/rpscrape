import boto3

from RPScraper.src.utils.config import get_attribute

PROJECT_DIR = get_attribute('PROJECT_DIR')
S3_BUCKET = get_attribute('S3_BUCKET')

AWS_GLUE_DB = get_attribute('AWS_GLUE_DB')
AWS_GLUE_TABLE = get_attribute('AWS_GLUE_TABLE')

AWS_ACCESS_KEY_ID = get_attribute('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_attribute('AWS_SECRET_ACCESS_KEY')

boto3_session = boto3.session.Session(
     aws_access_key_id=AWS_ACCESS_KEY_ID,
     aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

SCHEMA_COLUMNS = {
     'date': 'timestamp',
     'course': 'string',
     'off': 'string',
     'name': 'string',
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
     'sp': 'string',
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
     "race_id": 'int',
     "horse_id": 'int',
     "jockey_id": 'int',
     "trainer_id": 'int',
     "owner_id": 'int',
     "dam_id": 'int',
     "damsire_id": 'int',
     "secs": 'double',
     "silk_url": 'string',
     'horse_cleaned': 'string',
     'jockey_cleaned': 'string',
     'trainer_cleaned': 'string',
     'year': 'int'
 }
