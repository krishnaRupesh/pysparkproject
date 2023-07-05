from base_file import BaseTask


class DailyTxnReportTask(BaseTask):
    """base task"""

    def __init__(self):
        self.dag = None
        self.dag_specific_attr = {
            'max_active_runs': 1,
            'catchup': False,
            'schedule_interval': '30 5 * * *'
        }

        self.launch_emr_cluster(emr_conn_id='emr_billing_report')

tp = DailyTxnReportTask()