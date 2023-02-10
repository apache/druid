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

import requests, time
from .consts import COORD_BASE
from .rest import check_error
from .util import dict_get

REQ_DATASOURCES = COORD_BASE + '/datasources'
REQ_DATASOURCE = REQ_DATASOURCES + '/{}'

# Segment load status
REQ_DATASOURCES = COORD_BASE + '/datasources'
REQ_DS_LOAD_STATUS = REQ_DATASOURCES + '/{}/loadstatus'

class DatasourceClient:
    '''
    Client for status APIs. These APIs are available on all nodes.
    If used with the router, they report the status of just the router.
    '''
    
    def __init__(self, rest_client):
        self.client = rest_client

    def names(self, include_unused=False, include_disabled=False):
        """
        Returns a list of the names of data sources in the metadata store.
        
        Parameters
        ----------
        include_unused : bool, default = False
            if False, returns only datasources with at least one used segment
            in the cluster.

        include_unused : bool, default = False
            if False, returns only enamed datasources.

        Reference
        ---------
        * `GET /druid/coordinator/v1/metadata/datasources`
        * `GET /druid/coordinator/v1/metadata/datasources?includeUnused`
        * `GET /druid/coordinator/v1/metadata/datasources?includeDisabled`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-4

        Note: this method uses a semi-deprecated API.
        See `Metadata.user_table_names()` for the preferred solution.
        """
        params = {}
        if include_unused:
            params['includeUnused'] = ''
        if include_disabled:
            params['includeDisabled'] = ''
        return self.client.get_json(REQ_DATASOURCES, params=params)
    
    def drop(self, ds_name, ifExists=False):
        """
        Drops a data source.

        Marks as unused all segments belonging to a datasource. 

        Marking all segments as unused is equivalent to dropping the table.
        
        Parameters
        ----------
        ds_name: str
            name of the datasource to query

        Returns
        -------
        Returns a map of the form 
        {"numChangedSegments": <number>} with the number of segments in the database whose 
        state has been changed (that is, the segments were marked as unused) as the result 
        of this API call.

        Reference
        ---------
        `DELETE /druid/coordinator/v1/datasources/{dataSourceName}`
        """
        r = self.client.delete(REQ_DATASOURCE, args=[ds_name])
        if ifExists and r.status_code == requests.codes.not_found:
            return
        check_error(r)

    def load_status_req(self, ds_name, params=None):
        return self.rest_client.get_json(REQ_DS_LOAD_STATUS, args=[ds_name], params=params)
    
    def load_status(self, ds_name):
        return self.load_status_req(ds_name, {
            'forceMetadataRefresh': 'true', 
            'interval': '1970-01-01/2999-01-01'})

    def wait_until_ready(self, ds_name):
        while True:
            resp = self.load_status(ds_name)
            if dict_get(resp, ds_name) == 100.0:
                return
            time.sleep(0.5)
            