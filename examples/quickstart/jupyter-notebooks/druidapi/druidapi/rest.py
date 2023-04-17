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
from druidapi.util import dict_get
from urllib.parse import quote
from druidapi.error import ClientError

def check_error(response):
    '''
    Raises an HttpError from the requests library if the response code is neither
    OK (200) nor Accepted (202).

    Druid's REST API is inconsistent with how it reports errors. Some APIs return
    an error as a JSON object. Others return a text message. Still others return
    nothing at all. With the JSON format, sometimes the error returns an
    'errorMessage' field, other times only a generic 'error' field.

    This method attempts to parse these variations. If the error response JSON
    matches one of the known error formats, then raises a `ClientError` with the error
    message. Otherise, raises a Requests library `HTTPError` for a generic error.
    If the response includes a JSON payload, then the it is returned in the json field
    of the `HTTPError` object so that the client can perhaps decode it.
    '''
    code = response.status_code
    if code == requests.codes.ok or code == requests.codes.accepted:
        return
    json = None
    try:
        json = response.json()
    except Exception:
        # If we can't get the JSON, raise a Requests error
        response.raise_for_status()

    # Druid JSON payload. Try to make sense of the error
    msg = dict_get(json, 'errorMessage')
    if not msg:
        msg = dict_get(json, 'error')
    if msg:
        # We have an explanation from Druid. Raise a Client exception
        raise ClientError(msg)

    # Don't know what the Druid JSON is. Raise a Requests exception, but
    # add on the JSON in the hopes that the caller can make use of it.
    try:
        response.raise_for_status()
    except Exception as e:
        e.json = json
        raise e

def build_url(endpoint, req, args=None) -> str:
    '''
    Returns the full URL for a REST call given the relative request API and
    optional parameters to fill placeholders within the request URL.

    Parameters
    ----------
    endpoint: str
        The base URL for the service.

    req: str
        Relative URL, with optional {} placeholders

    args: list
        Optional list of values to match {} placeholders in the URL.
    '''
    url = endpoint + req
    if args:
        quoted = [quote(arg) for arg in args]
        url = url.format(*quoted)
    return url

class DruidRestClient:
    '''
    Wrapper around the basic Druid REST API operations using the
    requests Python package. Handles the grunt work of building up
    URLs, working with JSON, etc.

    The REST client accepts an endpoint that represents a Druid service, typically
    the Router. All requests are made to this service, which means using the service
    URL as the base. That is, if the service is http://localhost:8888, then
    a request for status is just '/status': the methods here build up the URL by
    concatenating the service endpoint with the request URL.
    '''

    def __init__(self, endpoint, auth=None):
        '''
        Creates a Druid rest client endpoint using the given endpoint URI and
        optional authentication.

        Parameters
        ----------
        endpoint: str
            The Druid router endpoint of the form `'server:port'`. Use
            `'localhost:8888'` for a Druid instance running locally.

        auth: str, default = None
            Optional authorization credentials in the format described
            by the Requests library. For Basic auth use
            `auth=('user', 'password')`
        '''
        self.endpoint = endpoint
        self.trace = False
        self.session = requests.Session()
        if auth:
            self.session.auth = auth

    def enable_trace(self, flag=True):
        self.trace = flag

    def build_url(self, req, args=None) -> str:
        '''
        Returns the full URL for a REST call given the relative request API and
        optional parameters to fill placeholders within the request URL.

        Parameters
        ----------
        req: str
            Relative URL, with optional {} placeholders

        args: list
            Optional list of values to match {} placeholders in the URL.
        '''
        return build_url(self.endpoint, req, args)

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
            the URL. Query parameters are the name/value pairs
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
            print('GET:', url)
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

    def post(self, req, body, args=None, headers=None, require_ok=True) -> requests.Response:
        '''
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query
        parameters.
        '''
        url = self.build_url(req, args)
        if self.trace:
            print('POST:', url)
            print('body:', body)
        r = self.session.post(url, data=body, headers=headers)
        if require_ok:
            check_error(r)
        return r

    def post_json(self, req, body, args=None, headers=None, params=None) -> requests.Response:
        '''
        Issues a POST request for the given URL on this node, with a JSON request. Returns
        the JSON response.

        Parameters
        ----------
        req: str
            URL relative to the service base URL.

        body: any
            JSON-encodable Python object to send in the request body.

        args: array[str], default = None
            Arguments to include in the relative URL to replace {} markers.

        headers: dict, default = None
            Additional HTTP header fields to send in the request.

        params: dict, default = None
            Parameters to inlude in the URL as the `?name=value` query string.

        Returns
        -------
            The JSON response as a Python object.

        See
        ---
            `post_only_json()` for the form that returns the response object, not JSON.
        '''
        r = self.post_only_json(req, body, args, headers, params)
        check_error(r)
        return r.json()

    def post_only_json(self, req, body, args=None, headers=None, params=None, require_ok=True) -> requests.Request:
        '''
        Issues a POST request for the given URL on this node, with a JSON request, returning
        the Requests library `Response` object.

        Parameters
        ----------
        req: str
            URL relative to the service base URL.

        body: any
            JSON-encodable Python object to send in the request body.

        args: array[str], default = None
            Arguments to include in the relative URL to replace {} markers.

        headers: dict, default = None
            Additional HTTP header fields to send in the request.

        params: dict, default = None
            Parameters to inlude in the URL as the `?name=value` query string.

        Returns
        -------
            The JSON response as a Python object.

        See
        ---
            `post_json()` for the form that returns the response JSON.
        '''
        url = self.build_url(req, args)
        if self.trace:
            print('POST:', url)
            print('body:', body)
        r = self.session.post(url, json=body, headers=headers, params=params)
        if require_ok:
            check_error(r)
        return r

    def delete(self, req, args=None, params=None, headers=None, require_ok=True):
        url = self.build_url(req, args)
        if self.trace:
            print('DELETE:', url)
        r = self.session.delete(url, params=params, headers=headers)
        if require_ok:
            check_error(r)
        return r

    def delete_json(self, req, args=None, params=None, headers=None):
        return self.delete(req, args=args, params=params, headers=headers).json()

    def close(self):
        '''
        Close the session. Use in scripts and tests when the system will otherwise complain
        about open sockets.
        '''
        self.session.close()
        self.session = None
