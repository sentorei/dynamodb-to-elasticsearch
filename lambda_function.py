import boto3
import urllib
import json
from decimal import Decimal

# definition
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION_NAME = ''
TABLE_NAME = ''
INDEX_NAME = ''
ES_INDEX = ''
ES_TYPE = ''
BUCKET_NAME = ''
ES_ENDPOINT = ''

def lambda_handler(event, context):
    ses = boto3.Session()
    if  AWS_ACCESS_KEY_ID and  AWS_SECRET_ACCESS_KEY and  REGION_NAME:
        ses = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=REGION_NAME)

    # dynamodb全権取得
    db = ses.resource("dynamodb")
    table = db.Table(TABLE_NAME)
    res = table.scan(
        TableName=TABLE_NAME,
        IndexName=INDEX_NAME,
    )
    record = res["Items"]

    while "LastEvaluatedKey" in res:
        res = table.scan(
            TableName=TABLE_NAME,
            IndexName=INDEX_NAME,
            ExclusiveStartKey=res["LastEvaluatedKey"]
        )
        record.extend(res["Items"])

    bulk = list()
    for item in record:
        bulk.append(str(json.dumps({"index": {"_index": ES_INDEX, "_type": ES_TYPE, }})))
        bulk.append(str(json.dumps(item, default=decimal_convert, ensure_ascii=False)))

    # create bulk file
    file = "/tmp/bulk.text"
    with open(file=file, mode="w") as f:
        f.write(
            "\n".join(bulk)
        )
        f.write("\n")

    # upload file to s3 if BUCKET_NAME is exists
    if BUCKET_NAME:
        s3 = ses.resource("s3")
        s3.Bucket(BUCKET_NAME).upload_file(file, "bulk.txt")

    # bulk post to Elasticsearch
    with open(file=file, mode="r") as f:
        req = urllib.request.Request(url=os.path.join(ES_ENDPOINT, '_bulk'),
                                     headers={"Content-Type": "application/x-ndjson"},
                                     data=f.read().encode("utf8"))
        with urllib.request.urlopen(req) as r:
            print(r.read().decode("utf-8"))


def decimal_convert(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError
    