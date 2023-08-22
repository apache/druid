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

from druidapi.consts import OVERLORD_BASE
import requests

REQ_TASKS = OVERLORD_BASE + '/tasks'
REQ_POST_TASK = OVERLORD_BASE + '/task'
REQ_GET_TASK = REQ_POST_TASK + '/{}'
REQ_TASK_STATUS = REQ_GET_TASK + '/status'
REQ_TASK_REPORTS = REQ_GET_TASK + '/reports'
REQ_END_TASK = REQ_GET_TASK
REQ_END_DS_TASKS = REQ_END_TASK + '/shutdownAllTasks'

class TaskClient:
    '''
    Client for Overlord task-related APIs.

    See https://druid.apache.org/docs/latest/api-reference/api-reference.html#tasks
    '''

    def __init__(self, rest_client):
        self.client = rest_client

    def tasks(self, state=None, table=None, task_type=None, max=None, created_time_interval=None):
        '''
        Retrieves the list of tasks.

        Parameters
        ----------
        state: str, default = None
            Filter list of tasks by task state. Valid options are "running",
            "complete", "waiting", and "pending". Constants are defined for
            each of these in the `consts` file.

        table: str, default = None
            Return tasks for only for one Druid table (datasource).

        created_time_interval: str, Default = None
            Return tasks created within the specified interval.

        max: int, default = None
            Maximum number of "complete" tasks to return. Only applies when state is set to "complete".

        task_type: str, default = None
            Filter tasks by task type.

        Reference
        ---------
        `GET /druid/indexer/v1/tasks`
        '''
        params = {}
        if state:
            params['state'] = state
        if table:
            params['datasource'] = table
        if task_type:
            params['type'] = task_type
        if max is not None:
            params['max'] = max
        if created_time_interval:
            params['createdTimeInterval'] = created_time_interval
        return self.client.get_json(REQ_TASKS, params=params)

    def task(self, task_id) -> dict:
        '''
        Retrieves the "payload" of a task.

        Parameters
        ----------
        task_id: str
            The ID of the task to retrieve.

        Returns
        -------
        The task payload as a Python dictionary.

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}`
        '''
        return self.client.get_json(REQ_GET_TASK, args=[task_id])

    def task_status(self, task_id) -> dict:
        '''
        Retrieves the status of a task.

        Parameters
        ----------
        task_id: str
            The ID of the task to retrieve.

        Returns
        -------
        The task status as a Python dictionary. See the `consts` module for a list
        of status codes.

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}/status`
        '''
        return self.client.get_json(REQ_TASK_STATUS, args=[task_id])

    def task_reports(self, task_id, require_ok = True) -> dict:
        '''
        Retrieves the completion report for a completed task.

        Parameters
        ----------
        task_id: str
            The ID of the task to retrieve.

        Returns
        -------
        The task reports as a Python dictionary.

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}/reports`
        '''
        if require_ok:
            return self.client.get_json(REQ_TASK_REPORTS, args=[task_id])
        else:
            resp = self.client.get(REQ_TASK_REPORTS, args=[task_id], require_ok=require_ok)
            if resp.status_code == requests.codes.ok:
                try:
                    result = resp.json()
                except Exception as ex:
                    result = {"message":"Payload could not be converted to json.", "payload":f"{resp.content}", "exception":f"{ex}"}
                return result
            else:
                return {"message":f"Request return code:{resp.status_code}"}


    def submit_task(self, payload):
        '''
        Submits a task to the Overlord.

        Returns the `taskId` of the submitted task.

        Parameters
        ----------
        payload: object
            The task object represented as a Python dictionary.

        Returns
        -------
        The REST response.

        Reference
        ---------
        `POST /druid/indexer/v1/task`
        '''
        return self.client.post_json(REQ_POST_TASK, payload)

    def shut_down_task(self, task_id):
        '''
        Shuts down a task.

        Parameters
        ----------
        task_id: str
            The ID of the task to shut down.

        Returns
        -------
            The REST response.

        Reference
        ---------
        `POST /druid/indexer/v1/task/{taskId}/shutdown`
        '''
        return self.client.post_json(REQ_END_TASK, args=[task_id])

    def shut_down_tasks_for(self, table):
        '''
        Shuts down all tasks for a table (datasource).

        Parameters
        ----------
        table: str
            The name of the table (datasource).

        Returns
        -------
        The REST response.

        Reference
        ---------
        `POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`
        '''
        return self.client.post_json(REQ_END_DS_TASKS, args=[table])
