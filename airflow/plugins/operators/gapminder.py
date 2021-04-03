import boto3
import pandas as pd
import configparser
from io import StringIO

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators  import apply_defaults


class GapminderOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                aws_credentials_id,
                s3_bucket,
                origin_s3_folder,
                destination_s3_folder,
                *args,
                **kwargs):
        """
        init method

        args:
            self:
            origin_s3_folder:
            destination_s3_folder:
            *args:
            **kwargs:
        """
        super(GapminderOperator, self).__init__(*args, **kwargs)

        # set attributes
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.origin_s3_folder = origin_s3_folder
        self.destination_s3_folder = destination_s3_folder


    def execute(self, context):
        """
        Read gapminder source files and join them into a
        new output file. Stored output file in s3 destination folder.

        args:
            self
            context
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info("read and process gapminder source data files from s3 origin folder")
        s3 = boto3.client('s3', aws_access_key_id = credentials.access_key,
                         aws_secret_access_key = credentials.secret_key,
                         region_name='us-west-2'
                      )

        response = s3.list_objects(Bucket=self.s3_bucket, Prefix=self.origin_s3_folder)

        self.log.info("merge source gapminder source files into new destination file")
        df_merge = None
        for resp_obj in response['Contents']:
            rkey = resp_obj['Key']
            self.log.info(f"reading {rkey} file")

            obj = s3.get_object(Bucket=self.s3_bucket, Key=rkey)
            fileobj = obj['Body']
            df = pd.read_csv(fileobj)

            if df_merge is None:
                df_merge = df
            else:
                df_merge = df_merge.merge(df, on=['geo','time'], how='outer')

        self.log.info("create new demographics file in destination s3 folder")

        # Create buffer
        csv_buffer = StringIO()

        # Write dataframe to buffer
        df_merge.to_csv(csv_buffer, index=False)

        # Write buffer to S3 object
        s3.put_object(Bucket=self.s3_bucket, Key=self.destination_s3_folder, Body=csv_buffer.getvalue())
