from airflow.decorators import task, task_group
from nghiatt2.core.ProcessControl import ExtractControl
from nghiatt2.tasks.base_task_flow import BaseTaskGroup
import pyodbc

class ExtractTaskGroup(BaseTaskGroup):

    def __init__(self,task_group_name,task_suite_group):
        super().__init__(ExtractControl())
        self.task_group_name = task_group_name
        self.task_suite_group = task_suite_group

    def get_extract_control_list(self):
        extract_info_list,suite_group_list = self.control_class.get_extract_package_list(self.task_suite_group)
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Extract Manage Get Extract Package List Start",
                message = "",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )
        return {"ListExtractInfo" : extract_info_list, "ListSuiteGroup" : suite_group_list}
    
    def log_extract_error_package_count(self,extract_package_list):
        message_log = extract_package_list.replace("'","''") if isinstance(extract_package_list,str) else None
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 0,
                complete_flag = 1,
                message_source = "Log Extract Manage Has ERROR",
                message = f"Check Instance: {message_log}",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )

    def process_package(self,package_name,suite_group,extract_standard_variable):
        extract_job_id,staging_log_info,source_table,source_columns,extract_merge_query,extract_table_keys,extract_merge_condition_query,spark_execution_path,spark_submit_remote,iceberg_catalog_uri,warehouse_path = extract_standard_variable[:11]
        spark_session_configs = f"batch_raw2ext;{iceberg_catalog_uri};{warehouse_path}"
        unix_arguments = [fr"{spark_execution_path} '{spark_session_configs}' '{source_table}' '{source_columns}' '{extract_merge_query}' '{extract_table_keys}' '{extract_merge_condition_query}' '{staging_log_info}'"]
        message_log = (" ".join(unix_arguments)).replace("'","''")
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                extract_job_id = extract_job_id,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log ExtractExecution System Command Starting",
                message = f"{message_log}",
                extract_control_id = -1,
                staging_table_name = package_name,
                message_type = None
            )
        )

        command_status,command_result = self.general_function.call_spark_submit(spark_submit_remote,unix_arguments)
        message_log = command_result.replace("'","''")
        if command_status == "OK":
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    extract_job_id = extract_job_id,
                    start_time = self.execution_start_time,
                    success_flag = 1,
                    complete_flag = 1,
                    message_source = "Log Extract Execution System Command Has Been Completed",
                    message = f"{message_log}",
                    extract_control_id = -1,
                    staging_table_name = package_name,
                    message_type = None,
                    suite_group = suite_group
                )
            )
        else:
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    extract_job_id = extract_job_id,
                    start_time = self.execution_start_time,
                    success_flag = 0,
                    complete_flag = 1,
                    message_source = "Log Extract Execution System Command Has ERROR",
                    message = f"{message_log}",
                    extract_control_id = -1,
                    staging_table_name = package_name,
                    message_type = None,
                    suite_group = suite_group
                )
            )

    def log_extract_package_count(self,extract_package_list,suite_group_list):

        extract_package_list_object = (";".join([str(element) for element in extract_package_list])).replace("'","''")
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Extract Manage Package Count: {len(extract_package_list)}",
                message = f"List Extract Package: [{extract_package_list_object}]",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )
    
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Extract Execution Starting",
                message = f"{suite_group_list}:{extract_package_list}",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )
        
        for package_name,suite_group in zip(extract_package_list,suite_group_list):
            suite_group = suite_group.strip()
            extract_standard_variable = self.control_class.get_extract_execution_standard_variables(package_name,suite_group)
            self.process_package(package_name,suite_group,extract_standard_variable)

        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Extract Execution Has Been Completed",
                message = "",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )
        
    def handle_extract_package_list(self):

        extract_control_list_object = self.get_extract_control_list()
        extract_package_list = extract_control_list_object["ListExtractInfo"]
        suite_group_list = extract_control_list_object["ListSuiteGroup"]

        self.check_object (
            extract_package_list, 
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    extract_job_id = -1,
                    start_time = self.execution_start_time,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = "Log Extract Manage Has Been Completed",
                    message = "Extract Package List Empty",
                    extract_control_id = -1,
                    staging_table_name = "",
                    message_type = None
                )
            ),
            self.check_valid_list (  
                extract_package_list,
                self.log_extract_error_package_count(extract_package_list),
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        extract_job_id = -1,
                        start_time = self.execution_start_time,
                        success_flag = 1,
                        complete_flag = 0,
                        message_source = "Log Extract Manage Has Been Completed",
                        message = "",
                        extract_control_id = -1,
                        staging_table_name = "",
                        message_type = None
                    )
                ),
                self.log_extract_package_count(extract_package_list,suite_group_list)
            )                   
        )

@task_group()

def extract_task_group(task_group_name,task_suite_group):
    extract_task_group = ExtractTaskGroup(task_group_name,task_suite_group)

    @task(task_id="LogExtractManageStart",trigger_rule="none_failed_min_one_success")
    def log_extract_manage_start_task():
        extract_task_group.control_class.execute_stored_procedure (
            extract_task_group.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = extract_task_group.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Extract Manage Start",
                message = "",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )
    
    @task(task_id = "HandleListOfExtractPackage")
    def handle_extract_package_list_task():
        extract_task_group.handle_extract_package_list()

    @task(task_id="LogExtractManageComplete")
    def log_extract_manage_complete_task():
        extract_task_group.control_class.execute_stored_procedure (
            extract_task_group.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = extract_task_group.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Extract Manage Has Been Completed",
                message = "",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )

    try:
        task_log_extract_control_start = log_extract_manage_start_task()
        task_handle_extract_package_list = handle_extract_package_list_task()
        task_log_extract_control_end = log_extract_manage_complete_task()
        task_log_extract_control_start >> task_handle_extract_package_list >> task_log_extract_control_end

    except (Exception, pyodbc.DatabaseError) as error:
        message_log = str(error).replace("'", "''")
        extract_task_group.control_class.execute_stored_procedure (
            extract_task_group.get_execution_log_procedure (
                extract_job_id = -1,
                start_time = extract_task_group.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Extract Manage Exception Has ERROR",
                message = f"{message_log}",
                extract_control_id = -1,
                staging_table_name = "",
                message_type = None
            )
        )