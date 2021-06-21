import boto3
import json
from datetime import datetime
from datetime import timedelta
import time
import os
import logging
import base64
from botocore.exceptions import ClientError

REGION_NAME="<REGION_NAME>"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
secret = ''

def get_secret():
    secret_name = "<SECRET_NAME>"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION_NAME
    )

    try:
        
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        
    except ClientError as e:
        
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            global secret
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

get_secret()
secret_json = json.loads(secret)
AWS_ACCESS_KEY_ID = secret_json['accessKeyId']
AWS_SECRET_ACCESS_KEY = secret_json['secretKey']


kvs = boto3.client("kinesisvideo")
ddb_client = boto3.client('dynamodb',region_name=REGION_NAME,aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def lambda_handler(event, context):
    res = ""

    logger.info(event)
    _body = json.loads(event['body'])
     
    if event['httpMethod'] == 'PUT' :
        id = _body['clientID']
        deviceID = _body['deviceID']
        streamName = _body['streamName']
        begTime = _body['begTime']
        endTime = _body['endTime']
        duration = _body['duration']
    
        res = save_dynamodb_tb(id,deviceID,begTime,endTime,duration,streamName)
        logger.info("save dynamodb table...")
        
    else:
        if event['httpMethod'] == 'GET' :
            
            streamName = _body['streamName']
            begTime = _body['begTime']
            endTime = _body['endTime']
            duration = _body['duration']
            
            descStream =kvs.describe_stream(StreamName=streamName)
            logger.info(descStream)
            
            Stream_ARN = descStream['StreamInfo']['StreamARN']
            logger.info(Stream_ARN)
    
            get_hls_response = kvs.get_data_endpoint(APIName="GET_HLS_STREAMING_SESSION_URL",StreamARN=Stream_ARN)
            hls_endpoint = get_hls_response['DataEndpoint']
            
            kvs_client = boto3.client("kinesis-video-archived-media",
                                endpoint_url=hls_endpoint,
                                region_name=REGION_NAME,
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                                
            res = get_hls_url(kvs_client,streamName,begTime,endTime,duration)
            
    return {
        'statusCode': 200,
        'body': json.dumps(res)
        #'body': json.dumps('Hello from Lambda!')
    }


def get_hls_url(client,streamName,begTime,endTime,duration):
    return client.get_hls_streaming_session_url(StreamName=streamName, PlaybackMode="ON_DEMAND",
                                                        HLSFragmentSelector={
                                                            'FragmentSelectorType': 'PRODUCER_TIMESTAMP',
                                                            'TimestampRange':{
                                                                'EndTimestamp':datetime.fromtimestamp(int(endTime)/1000),
                                                                'StartTimestamp':datetime.fromtimestamp(int(endTime)/1000) - timedelta(seconds=int(duration))
                                                                
                                                                
                                                            }
                                                        },
                                                        ContainerFormat='FRAGMENTED_MP4',#MPEG_TS,FRAGMENTED_MP4
                                                        DiscontinuityMode='ON_DISCONTINUITY',
                                                        DisplayFragmentTimestamp='ALWAYS',
                                                        Expires=7200
                                                        )['HLSStreamingSessionURL']
                                                        

def save_dynamodb_tb(id,deviceID,begTime,endTime,duration,streamName):
    response = ddb_client.put_item(
    TableName='tb_device_metadata',
    Item={
        'id': {'S': id},
        'deviceID': {'S': deviceID},
        'streamName': {'S': streamName},
        'begTime': {'S': begTime},
        'endTime': {'S': endTime},
        'videoDuration':{'S':duration},
        'datetime': {'S': datetime.utcnow().strftime("%Y%m%d%H%M%S")}
        }
    )
    
def read_dynamodb_tb(deviceID,begTime,endTime):
    response = ddb_client.get_item(
    TableName='tb_device_metadata',
    Item={
        'deviceID': {'S': deviceID},
        'begTime': {'S': begTime},
        'endTime': {'S': endTime}
        }
    )
