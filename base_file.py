class BaseTask:
    """base task"""

    def __init__(self):
        self.dag = None

    def launch_emr_cluster(self, aws_conn_id='aws_default', emr_conn_id='emr_default'):
        aws_conn_id=aws_conn_id
        emr_conn_id=emr_conn_id
        print(aws_conn_id)
        print(emr_conn_id)
