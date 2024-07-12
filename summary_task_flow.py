from datetime import datetime
from airflow.decorators import task, task_group
from nghiatt2.core.ProcessControl import SummaryControl
from nghiatt2.core.parse.SummaryControlParsing import ParseSummaryControlVariable
from nghiatt2.tasks.base_task_flow import BaseTaskGroup

class SummaryTaskGroup(BaseTaskGroup):
    def __init__(self,task_group_name,task_suite_code):
        super().__init__(SummaryControl())
        self.task_group_name = task_group_name
        self.task_suite_code = task_suite_code

    def process_summary_control(self,summary_control_id,suite_code):
        summary_job_id = -1
        message_log = f"SummaryControlID = {summary_control_id}"

        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                summary_control_id = summary_control_id,
                summary_job_id = summary_job_id,
                start_time = self.execution_start_time,
                summary_start_time = None,
                summary_end_time = None,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Summary Execution Starting",
                message = message_log,
                suite_code = suite_code,
                message_type = None
            )
        )

        summary_standard_variables = self.control_class.get_summary_execution_standard_variables(summary_control_id)

        if summary_standard_variables is not None:
            message_log = (repr(summary_standard_variables)).replace("'", "''")
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    summary_control_id = summary_control_id,
                    summary_job_id = summary_job_id,
                    start_time = self.execution_start_time,
                    summary_start_time = None,
                    summary_end_time = None,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = "Log Summary Execution Get Standard Variable Values",
                    message = message_log,
                    suite_code = suite_code,
                    message_type = None
                )
            )

            summary_config_info = ParseSummaryControlVariable(summary_standard_variables)

            summary_job_id = summary_config_info.summary_job_id
            enrich_query_path = summary_config_info.enrich_query_path
            summary_package_name = summary_config_info.summary_package_name
            summary_start_date = summary_config_info.summary_start_date
            summary_end_date = summary_config_info.summary_end_date
            summary_status = summary_config_info.status
            summary_package_count = summary_config_info.summary_package_count
            total_staging_success_count = summary_config_info.total_staging_success_count
            spark_remote_ip = summary_config_info.spark_remote_ip
            spark_execution_path = summary_config_info.spark_execution_path
            iceberg_catalog_uri = summary_config_info.iceberg_catalog_uri
            warehouse_path = summary_config_info.warehouse_path
            spark_session_configurations = f"ext2enrich;{iceberg_catalog_uri};{warehouse_path}"

            if summary_status == "S":
                from_partition_key = datetime.fromisoformat(str(summary_start_date)).strftime("%Y%m")
                to_partition_key = datetime.fromisoformat(str(summary_end_date)).strftime("%Y%m")
                spark_arguments = [fr"{spark_execution_path} '{spark_session_configurations}' '{enrich_query_path}' '{summary_package_name}' '{summary_start_date}' '{summary_end_date}' '{summary_job_id}' '{from_partition_key}' '{to_partition_key}'"]
                message_log = (" ".join(spark_arguments)).replace("'", "''")
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        summary_control_id = summary_control_id,
                        summary_job_id = summary_job_id,
                        start_time = self.execution_start_time,
                        summary_start_time = summary_start_date,
                        summary_end_time = summary_end_date,
                        success_flag = 1,
                        complete_flag = 0,
                        message_source = "Log SummaryExecution Spark-Submit Started",
                        message = message_log,
                        suite_code = suite_code,
                        message_type = None
                    )
                )

                spark_status, spark_result = self.general_function.call_spark_submit(spark_remote_ip,spark_arguments)
                if spark_status == "OK":
                    self.control_class.execute_stored_procedure (
                        self.get_execution_log_procedure (
                            summary_control_id = summary_control_id,
                            summary_job_id = summary_job_id,
                            start_time = self.execution_start_time,
                            summary_start_time = summary_start_date,
                            summary_end_time = summary_end_date,
                            success_flag = 1,
                            complete_flag = 1,
                            message_source = f"Log Summary Execution Enrich {summary_package_name} Completed",
                            message = spark_result.replace("'","''"),
                            suite_code = suite_code,
                            message_type = None
                        )
                    )
                else:
                    self.control_class.execute_stored_procedure (
                        self.get_execution_log_procedure (
                            summary_control_id = summary_control_id,
                            summary_job_id = summary_job_id,
                            start_time = self.execution_start_time,
                            summary_start_time = summary_start_date,
                            summary_end_time = summary_end_date,
                            success_flag = 0,
                            complete_flag = 1,
                            message_source = f"Log Summary Execution Enrich {summary_package_name} ERROR: ",
                            message = spark_result.replace("'","''"),
                            suite_code = suite_code,
                            message_type = None
                        )
                    )
            else:
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        summary_control_id = summary_control_id,
                        summary_job_id = summary_job_id,
                        start_time = self.execution_start_time,
                        summary_start_time = None,
                        summary_end_time = None,
                        success_flag = 1,
                        complete_flag = 0,
                        message_source = "Log Summary Execution Has Been Completed",
                        message = f"Package Count {summary_package_name} Has Closed: packageCount:{summary_package_count} != listTotalStagingCount:{total_staging_success_count} Or summary_start_date != summary_end_date",
                        suite_code = suite_code,
                        message_type = None
                    )
                )
        else:
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    summary_control_id = summary_control_id,
                    summary_job_id = summary_job_id,
                    start_time = self.execution_start_time,
                    summary_start_time = None,
                    summary_end_time = None,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = "Log Summary Execution Has Been Completed",
                    message = "Get Standard Variable Values Has Closed",
                    suite_code = suite_code,
                    message_type = None
                )
            )

    def handle_list_summary_suite(self,suite_code):
        summary_control_ids = self.control_class.get_next_datetime_summary_id(suite_code)
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                summary_control_id = -1,
                summary_job_id = -1,
                start_time = self.execution_start_time,
                summary_start_time = None,
                summary_end_time = None,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Summary Manage Get List SummaryControlID",
                message = f"SuiteCode: {suite_code}",
                suite_code = suite_code,
                message_type = None
            )
        )

        if len(summary_control_ids) == 0 or not summary_control_ids:
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    summary_control_id = -1,
                    summary_job_id = -1,
                    start_time = self.execution_start_time,
                    summary_start_time = None,
                    summary_end_time = None,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = "Log Summary Manage Get List SummaryControlID Empty",
                    message = "",
                    suite_code = suite_code,
                    message_type = None
                )
            )
        else:
            message_log = f"List SummaryControlID = [{(";".join([str(element) for element in summary_control_ids])).replace("'", "''")}]"
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    summary_control_id = -1,
                    summary_job_id = -1,
                    start_time = self.execution_start_time,
                    summary_start_time = None,
                    summary_end_time = None,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = f"Log Summary Manage Package Count: {len(summary_control_ids)}",
                    message = message_log,
                    suite_code = suite_code,
                    message_type = None
                )
            )

            for summary_control_id in summary_control_ids:
                self.process_summary_control(summary_control_id,suite_code)
    
@task_group()
def summary_task_group(task_group_name, task_suite_code):
    summary_task_group = SummaryTaskGroup(task_group_name, task_suite_code)

    @task(task_id="LogSummaryManageStart", trigger_rule="none_failed_min_one_success")
    def log_summary_manager_start_task():
        summary_task_group.control_class.execute_stored_procedure (
            summary_task_group.get_execution_log_procedure (
                summary_control_id = -1,
                summary_job_id = -1,
                start_time = summary_task_group.execution_start_time,
                summary_start_time = None,
                summary_end_time = None,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Summary Manage Start",
                message = message_log,
                suite_code = None,
                message_type = None
            )
        )

    @task(task_id="HandleListSummarySuite")
    def handle_list_summary_suite_task():
        summary_task_group.handle_list_summary_suite(summary_task_group.task_suite_code)

    @task(task_id="LogSummaryManagerComplete")
    def log_summary_manager_complete_task():
        summary_task_group.control_class.execute_stored_procedure (
            summary_task_group.get_execution_log_procedure (
                summary_control_id = -1,
                summary_job_id = -1,
                start_time = summary_task_group.execution_start_time,
                summary_start_time = None,
                summary_end_time = None,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Summary Manage Has Been Completed",
                message = "",
                suite_code = None,
                message_type = None
            )
        )

    try:
        task_log_summary_control_start = log_summary_manager_start_task()
        task_handle_list_summary_suite = handle_list_summary_suite_task()
        task_log_summary_control_end = log_summary_manager_complete_task()

        task_log_summary_control_start >> task_handle_list_summary_suite >> task_log_summary_control_end

    except Exception as error:
        message_log = (str(error)).replace("'", "''")
        summary_task_group.control_class.execute_stored_procedure (
            summary_task_group.get_execution_log_procedure (
                summary_control_id = -1,
                summary_job_id = -1,
                start_time = summary_task_group.execution_start_time,
                summary_start_time = None,
                summary_end_time = None,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Summary Manage Exception ERROR: ",
                message = message_log,
                suite_code = None,
                message_type = None
            )
        )
        raise error

        

