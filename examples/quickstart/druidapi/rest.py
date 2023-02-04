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

import requests
from .util import dict_get, is_blank
from urllib.parse import quote

def check_error(response):
    """
    Raises a requests HttpError if the response code is not OK or Accepted.

    If the response inclded a JSON payload, then the message is extracted
    from that payload, else the message is from requests. The JSON
    payload, if any, is returned in the json field of the error.
    """
    code = response.status_code
    if code == requests.codes.ok or code == requests.codes.accepted:
        return
    error = None
    json = None
    try:
        json = response.json()
        msg = dict_get(json, 'error')
        if not is_blank(msg):
            error = msg
    except Exception:
        pass
    if code == requests.codes.not_found and error is None:
        error = "Not found"
    if error is not None:
        response.reason = error
    try:
        response.raise_for_status()
    except Exception as e:
        e.json = json

class DruidRestClient:
    '''
    Wrapper around the basic Druid REST API operations using the
    requests Python package. Handles the grunt work of building up
    URLs, working with JSON, etc.
    '''
    
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.trace = False
        self.session = requests.Session()
        
    def enable_trace(self, flag=True):
        self.trace = flag

    def build_url(self, req, args=None) -> str:
        """
        Returns the full URL for a REST call given the relative request API and
        optional parameters to fill placeholders within the request URL.

        Parameters
        ----------
        req : str
            relative URL, with optional {} placeholders

        args : list
            optional list of values to match {} placeholders
            in the URL.
        """
        url = self.endpoint + req
        if args is not None:
            quoted = [quote(arg) for arg in args]
            url = url.format(*quoted)
        return url

    def get(self, req, args=None, params=None, require_ok=True) -> requests.Request:
        '''
        Generic GET request to this service.

        Parameters
        ----------
        req: str
            The request URL without host, port or query string.
            Example: `/status`

        args: [str], default = None
            Optional parameters to fill in to the URL.
            Example: `/customer/{}`

        params: dict, default = None
            Optional map of query variables to send in
            the URL. Query parameters are the name/values pairs
            that appear after the `?` marker.

        require_ok: bool, default = True
            Whether to require an OK (200) response. If `True`, and
            the request returns a different response code, then raises
            a `RestError` exception.

        Returns
        -------
        The `requests` `Request` object.
        '''
        url = self.build_url(req, args)
        if self.trace:
            print("GET:", url)
        r = self.session.get(url, params=params)
        if require_ok:
            check_error(r)
        return r

    def get_json(self, url_tail, args=None, params=None):
        '''
        Generic GET request which expects a JSON response.
        '''
        r = self.get(url_tail, args, params)
        return r.json()

    def post(self, req, body, args=None, headers=None, require_ok=True) -> requests.Request:
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters.
        """
        url = self.build_url(req, args)
        if self.trace:
            print("POST:", url)
            print("body:", body)
        r = self.session.post(url, data=body, headers=headers)
        if require_ok:
            check_error(r)
        return r

    def post_json(self, req, body, args=None, headers=None, params=None):
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.
        """
        r = self.post_only_json(req, body, args, headers, params)
        check_error(r)
        return r.json()

    def post_only_json(self, req, body, args=None, headers=None, params=None) -> requests.Request:
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.

        Does not parse error messages: that is up to the caller.
        """
        url = self.build_url(req, args)
        if self.trace:
            print("POST:", url)
            print("body:", body)
        return self.session.post(url, json=body, headers=headers, params=params)

    def delete(self, req, args=None, params=None, headers=None):
        url = self.build_url(req, args)
        if self.trace:
            print("DELETE:", url)
        r = self.session.delete(url, params=params, headers=headers)
        return r

    def delete_json(self, req, args=None, params=None, headers=None):
        return self.delete(req, args=args, params=params, headers=headers).json()
