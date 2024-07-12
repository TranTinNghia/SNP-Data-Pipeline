from airflow.decorators import task, task_group
from nghiatt2.core.ProcessControl import StagingControl
from nghiatt2.core.parse.StagingControlParsing import ParseStagingControlVariable
from nghiatt2.tasks.base_task_flow import BaseTaskGroup
import pyodbc

class StagingTaskGroup(BaseTaskGroup):
    def __init__(self,task_group_name,task_suite_code):
        super().__init__(StagingControl())
        self.task_group_name = task_group_name
        self.task_suite_code = task_suite_code

    def get_list_staging_control_id(self):
        staging_control_ids = self.control_class.get_list_staging_control_id(self.task_suite_code)
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Manage Get List Staging Control ID of {self.task_suite_code}",
                message = None,
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )
        return staging_control_ids

    def process_staging_object(self,staging_control_id):
        staging_job_id = staging_objects[7]
        staging_variables = ParseStagingControlVariable(staging_objects)
        next_staging_datetime = staging_variables.staging_end_time
        staging_start_time = staging_variables.staging_start_time
        staging_end_time = staging_variables.staging_end_time
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Execution {staging_variables.staging_table} Starting",
                message = "",
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = staging_control_id,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

        staging_objects = list(staging_objects).pop(5)
        message_log = (repr(staging_objects)).replace("'","''")
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = staging_job_id,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Execution Get Standard Variable Values",
                message = message_log,
                row_inserted = 0,
                staging_start_time = staging_start_time,
                staging_end_time = staging_end_time,
                staging_control_id = staging_control_id,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

        if staging_variables.source_type_name == "OLEDB_ORACLE":
            status,connection = self.control_class.db.connect_oracle_source(staging_variables.source_connection_string)
            if status == "ERR":
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        staging_job_id = staging_job_id,
                        start_time = self.execution_start_time,
                        success_flag = 0,
                        complete_flag = 1,
                        message_source = f"Log Staging Execution Connection to Source Has ERROR: ",
                        message = str(connection),
                        row_inserted = 0,
                        staging_start_time = staging_start_time,
                        staging_end_time = staging_end_time,
                        staging_control_id = staging_control_id,
                        suite_code = self.task_suite_code,
                        message_type = None,
                        next_staging_start_time = next_staging_datetime
                    )
                )
                return
        
        elif staging_variables.source_type_name == "OLEDB":
            status,connection = self.control_class.db.connect_mssql_source(staging_variables.source_connection_string)
            if status == "ERR":
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        staging_job_id = staging_job_id,
                        start_time = self.execution_start_time,
                        success_flag = 0,
                        complete_flag = 1,
                        message_source = f"Log Staging Execution Connection to Source Has ERROR: ",
                        message = str(connection),
                        row_inserted = 0,
                        staging_start_time = staging_start_time,
                        staging_end_time = staging_end_time,
                        staging_control_id = staging_control_id,
                        suite_code = self.task_suite_code,
                        message_type = None,
                        next_staging_start_time = next_staging_datetime
                    )
                )
                return
        
        message_log = (" ".join(staging_variables.sqoop_cmd_log)).replace("'","''")
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = staging_job_id,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Execution Performing Sqoop Bulk Copy Starting: ",
                message = message_log,
                row_inserted = 0,
                staging_start_time = staging_start_time,
                staging_end_time = staging_end_time,
                staging_control_id = staging_control_id,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = next_staging_datetime
            )
        )
            
        command_status,command_result = self.general_function.run_unix_command(staging_variables.sqoop_cmd)
        message_log = command_result.replace("'","''")

        if command_status == "OK":
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    staging_job_id = staging_job_id,
                    start_time = self.execution_start_time,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = f"Log Staging Execution Updating TMP_NextStagingRunTime",
                    message = f"{staging_variables.staging_table}:{next_staging_datetime}",
                    row_inserted = 0,
                    staging_start_time = staging_start_time,
                    staging_end_time = staging_end_time,
                    staging_control_id = staging_control_id,
                    suite_code = self.task_suite_code,
                    message_type = None,
                    next_staging_start_time = None
                )
            )
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    staging_job_id = staging_job_id,
                    start_time = self.execution_start_time,
                    success_flag = 1,
                    complete_flag = 1,
                    message_source = f"Log Staging Execution {staging_variables.staging_table} Has Been Completed",
                    message = message_log,
                    row_inserted = 0,
                    staging_start_time = staging_start_time,
                    staging_end_time = staging_end_time,
                    staging_control_id = staging_control_id,
                    suite_code = self.task_suite_code,
                    message_type = None,
                    next_staging_start_time = next_staging_datetime
                )
            )
        else:
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    staging_job_id = staging_job_id,
                    start_time = self.execution_start_time,
                    success_flag = 0,
                    complete_flag = 1,
                    message_source = f"Log Staging Execution Performing Sqoop Bulk Copy Has ERROR: ",
                    message = message_log,
                    row_inserted = 0,
                    staging_start_time = staging_start_time,
                    staging_end_time = staging_end_time,
                    staging_control_id = staging_control_id,
                    suite_code = self.task_suite_code,
                    message_type = None,
                    next_staging_start_time = next_staging_datetime
                )
            )

    def log_staging_package_count(self,staging_control_ids):
        message_log = f"List Of Staging Control ID: [{(",".join([str(element) for element in staging_control_ids])).replace("'","''")}]"
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Manage Package Count: {len(staging_control_ids)}",
                message = message_log,
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Execution Starting",
                message = None,
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

        for staging_control_id in staging_control_ids:
            staging_objects = self.control_class.get_staging_execution_standard_variables(staging_control_id)
            if staging_objects is not None:
                self.process_staging_object(staging_control_id)
            else:
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        staging_job_id = -1,
                        start_time = self.execution_start_time,
                        success_flag = 1,
                        complete_flag = 0,
                        message_source = f"Log Staging Execution Get Standard Variable Values Has Closed",
                        message = f"{staging_objects}",
                        row_inserted = 0,
                        staging_start_time = None,
                        staging_end_time = None,
                        staging_control_id = staging_control_id,
                        suite_code = self.task_suite_code,
                        message_type = None,
                        next_staging_start_time = None
                    )
                )
        
        self.control_class.execute_stored_procedure (
            self.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = self.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Execution Has Been Completed",
                message = None,
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = self.task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

    def handle_staging_package_list(self):
        staging_control_ids = self.get_list_staging_control_id()

        self.check_object (
            staging_control_ids,
            self.control_class.execute_stored_procedure (
                self.get_execution_log_procedure (
                    staging_job_id = -1,
                    start_time = self.execution_start_time,
                    success_flag = 1,
                    complete_flag = 0,
                    message_source = "Log Staging Manage Has Been Completed",
                    message = "List Of Staging Control ID Is Empty",
                    row_inserted = 0,
                    staging_start_time = None,
                    staging_end_time = None,
                    staging_control_id = -1,
                    suite_code = self.task_suite_code,
                    message_type = None,
                    next_staging_start_time = None
                )
            ),
            self.check_valid_list (
                staging_control_ids,
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        staging_job_id = -1,
                        start_time = self.execution_start_time,
                        success_flag = 0,
                        complete_flag = 1,
                        message_source = f"Log Staging Manage {self.task_suite_code} Has ERROR: ",
                        message = f"Check Instance List Of Staging Control ID: {staging_control_ids.replace("'","''") if isinstance(staging_control_ids,str) else None}",
                        row_inserted = 0,
                        staging_start_time = None,
                        staging_end_time = None,
                        staging_control_id = -1,
                        suite_code = self.task_suite_code,
                        message_type = None,
                        next_staging_start_time = None
                    )
                ),
                self.control_class.execute_stored_procedure (
                    self.get_execution_log_procedure (
                        staging_job_id = -1,
                        start_time = self.execution_start_time,
                        success_flag = 1,
                        complete_flag = 0,
                        message_source = "Log Staging Manage Has Been Completed",
                        message = None,
                        row_inserted = 0,
                        staging_start_time = None,
                        staging_end_time = None,
                        staging_control_id = -1,
                        suite_code = self.task_suite_code,
                        message_type = None,
                        next_staging_start_time = None
                    )
                ),
                self.log_staging_package_count(staging_control_ids)
            )
        )

@task_group()

def staging_task_group(task_group_name,task_suite_code):
    staging_task_group = StagingTaskGroup(task_group_name,task_suite_code)

    @task(task_id="LogStagingManageStart")
    def log_staging_manage_start_task():
        staging_task_group.control_class.execute_stored_procedure (
            staging_task_group.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = staging_task_group.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Staging Manage Starting",
                message = f"Suite {task_suite_code}",
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

    @task(task_id = "HandleListOfStagingPackage")
    def handle_staging_package_list_task():
        staging_task_group.handle_staging_package_list()

    @task(task_id="LogStagingManageComplete")
    def log_staging_manage_complete_task():
        staging_task_group.control_class.execute_stored_procedure (
            staging_task_group.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = staging_task_group.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = "Log Staging Manage Has Been Completed",
                message = None,
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )

    try:
        task_log_staging_control_start = log_staging_manage_start_task()
        task_handle_staging_package_list = handle_staging_package_list_task()
        task_log_staging_control_end = log_staging_manage_complete_task()
        task_log_staging_control_start >> task_handle_staging_package_list >> task_log_staging_control_end

    except (Exception, pyodbc.DatabaseError) as error:
        message_log = (str(error)).replace("'","''")
        staging_task_group.control_class.execute_stored_procedure (
            staging_task_group.get_execution_log_procedure (
                staging_job_id = -1,
                start_time = staging_task_group.execution_start_time,
                success_flag = 1,
                complete_flag = 0,
                message_source = f"Log Staging Manage Has ERROR: ",
                message = message_log,
                row_inserted = 0,
                staging_start_time = None,
                staging_end_time = None,
                staging_control_id = -1,
                suite_code = task_suite_code,
                message_type = None,
                next_staging_start_time = None
            )
        )