import boto3
import botocore
import json
import logging
import os

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def enable_replication(
        bucket_name,
        replication_role,
        dest_region,
        boto_s3_client=boto3.client('s3')):
    response = None
    try:
        response = boto_s3_client.get_bucket_replication(Bucket=bucket_name)
    except botocore.exceptions.ClientError:
        LOGGER.info("Replication not configed for bucket {} yet.".format(
            bucket_name
        ))
    if response is None or response['ReplicationConfiguration']['Rules'][0]['Status'] == 'Disabled':
        boto_s3_client.put_bucket_replication(
            Bucket=bucket_name,
            ReplicationConfiguration={
                'Role': replication_role,
                'Rules': [
                    {
                        'Prefix': '',
                        'Status': 'Enabled',
                        'Destination': {
                            'Bucket': "arn:aws:s3:::" + bucket_name + '-dr',
                            'StorageClass': 'STANDARD'
                        }
                    },
                ]
            }
        )
        LOGGER.info(
            'Cross Region Replication is enabled for bucket {}.'.format(
                bucket_name
            ))
    else:
        LOGGER.warning(
            'Cross Region Replication was already enabled for bucket {}.'.format(
                bucket_name
            ))

def handler(event, context):
    bucket_name = event['Records'][0]['Sns']['Message']
    enable_kms_encryption = os.environ['enable_kms_encryption'].lower()
    replication_role = os.environ['replication_role_arn']
    dest_region = os.environ['dest_region']

    enable_replication(bucket_name, replication_role, dest_region)
