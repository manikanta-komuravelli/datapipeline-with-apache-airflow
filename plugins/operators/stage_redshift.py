from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """This function is used to transfer data from S3 to staging tables in redshift database.
    
    Parameters:
    aws_credentials_id: Connection Id of the Airflow to Amazon Web Services
    redshift_conn_id: Connection Id of the Airflow to redshift database (postgres)
    table: destination staging table
    s3_bucket: name of S3 bucket, e.g. "udacity-dend"
    s3_key: name of S3 key. This field is templatable when context is enabled, e.g. "log_data/{execution_date.year}/{execution_date.month}/"
    delimiter: ,
    ignore_headers: '0' or '1'
    jsonpaths: path to JSONpaths file
    
    Returns: None
    """
    
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 jsonpaths="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.jsonpaths= jsonpaths

    def execute(self, context):
        self.log.info('StageToRedshiftOperator implementation to be started')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        json_option = self.jsonpaths or 'auto'
        delimiter_format = "FORMAT AS JSON '{}'".format(json_option)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            delimiter_format
        )
        redshift.run(formatted_sql)





