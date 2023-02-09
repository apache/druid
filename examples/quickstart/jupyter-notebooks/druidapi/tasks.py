# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .consts import OVERLORD_BASE

# Tasks
REQ_TASKS = OVERLORD_BASE + '/tasks'
REQ_POST_TASK = OVERLORD_BASE + '/task'
REQ_GET_TASK = REQ_POST_TASK + '/{}'
REQ_TASK_STATUS = REQ_GET_TASK + '/status'
REQ_TASK_REPORTS = REQ_GET_TASK + '/reports'
REQ_END_TASK = REQ_GET_TASK
REQ_END_DS_TASKS = REQ_END_TASK + '/shutdownAllTasks'

class TaskClient:
    """
    Client for task-related APIs. The APIs connect through the Router to
    the Overlord.
    """
    
    def __init__(self, rest_client):
        self.client = rest_client

    def tasks(self, state=None, table=None, type=None, max=None, created_time_interval=None):
        '''
        Retrieve list of tasks.

        Parameters
        ----------
        state : str, default = None
        	Filter list of tasks by task state. Valid options are "running", 
            "complete", "waiting", and "pending". Constants are defined for
            each of these in the `consts` file.
        table : str, default = None
        	Return tasks filtered by Druid table (datasource).
        created_time_interval : str, Default = None
        	Return tasks created within the specified interval.
        max	: int, default = None
            Maximum number of "complete" tasks to return. Only applies when state is set to "complete".
        type : str, default = None
        	filter tasks by task type.

        Reference
        ---------
        `GET /druid/indexer/v1/tasks`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        '''
        params = {}
        if state is not None:
            params['state'] = state
        if table is not None:
            params['datasource'] = table
        if type is not None:
            params['type'] = type
        if max is not None:
            params['max'] = max
        if created_time_interval is not None:
            params['createdTimeInterval'] = created_time_interval
        return self.client.get_json(REQ_TASKS, params=params)

    def task(self, task_id):
        """
        Retrieve the "payload" of a task.

        Parameters
        ----------
        task_id : str
            The id of the task to retrieve

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        """
        return self.client.get_json(REQ_GET_TASK, args=[task_id])

    def task_status(self, task_id):
        '''
        Retrieve the status of a task.

        Parameters
        ----------
        task_id : str
            The id of the task to retrieve

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        '''
        return self.client.get_json(REQ_TASK_STATUS, args=[task_id])

    def task_reports(self, task_id):
        '''
        Retrieve a task completion report for a task.
        Only works for completed tasks.

        Parameters
        ----------
        task_id : str
            The id of the task to retrieve

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}/reports`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        '''
        return self.client.get_json(REQ_TASK_REPORTS, args=[task_id])

    def submit_task(self, payload):
        """
        Submit a task or supervisor specs to the Overlord.
        
        Returns the taskId of the submitted task.

        Parameters
        ----------
        payload : object
            The task object. Serialized to JSON.

        Reference
        ---------
        `POST /druid/indexer/v1/task`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-5
        """
        return self.client.post_json(REQ_POST_TASK, payload)

    def shut_down_task(self, task_id):
        """
        Shuts down a task.

        Parameters
        ----------
        task_id : str
            The id of the task to shut down
        
        Reference
        ---------
        `POST /druid/indexer/v1/task/{taskId}/shutdown`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-5
        """
        return self.client.post_json(REQ_END_TASK, args=[task_id])

    def shut_down_tasks_for(self, table):
        """
        Shuts down all tasks for a table (data source).

        Parameters
        ----------
        table : str
            The name of the table (data source).
        
        Reference
        ---------
        `POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-5
        """
        return self.client.post_json(REQ_END_DS_TASKS, args=[table])

