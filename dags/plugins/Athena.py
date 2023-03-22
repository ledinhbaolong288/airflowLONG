from pyathena import connect
from dotenv import load_dotenv
from pyathena.cursor import DictCursor
import os
load_dotenv()


def processAthena(query, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    # cursor = connect(aws_access_key_id=os.getenv('aws_access_key_id'), aws_secret_access_key=os.getenv('aws_secret_access_key'),
    cursor = connect(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                     s3_staging_dir="s3://longbuckets3/data_read/",
                     region_name="us-east-1",
                     cursor_class=DictCursor).cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    return data
