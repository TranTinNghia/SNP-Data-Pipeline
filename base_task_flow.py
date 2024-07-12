from datetime import datetime
import re
from nghiatt2.core.GeneralFunction import GeneralFunction
from nghiatt2.core.ProcessControl import *
from nghiatt2.core.parse.StagingControlParsing import ParseStagingControlVariable
from nghiatt2.core.parse.SummaryControlParsing import ParseSummaryControlVariable
from nghiatt2.core.parse.DeliveryControlParsing import ParseDeliveryControlVariable

class BaseTaskGroup:
    def __init__(self, control_class):
        self.control_class = control_class

        if isinstance(self.control_class,StagingControl):
            self.control_name = "staging"
            self.control_class_variable = ParseStagingControlVariable
        elif isinstance(self.control_class,ExtractControl):
            self.control_name = "extract"
            self.control_class_variable = None
        elif isinstance(self.control_class,SummaryControl):
            self.control_name = "summary"
            self.control_class_variable = ParseSummaryControlVariable
        else:
            self.control_name = "delivery"
            self.control_class_variable = ParseDeliveryControlVariable

        self.execution_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.general_function = GeneralFunction()

    def get_execution_log_procedure(self,**kwargs):
        return getattr(self.control_class,f"get_{self.control_name}_execution_log_procedure")(**kwargs)

    def check_object(self,object,task_if_empty_list,task_if_not_empty_list):
        if len(object) == 0 or object is None:
            return [task_if_empty_list]
        else:
            return [task_if_not_empty_list]
        
    def check_valid_list(self,object,task_if_error,task_if_complete,task_if_list):
        if not isinstance(object,list):
            status = re.split(":",object)[0]
            if status == "Error" or status == "ERR":
                return [task_if_error]
            else:
                return [task_if_complete]
        else:
            return [task_if_list]
