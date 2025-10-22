/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package http

import (
	"bytes"
	"io"
	"net/http"
)

// DruidHTTP interface
type DruidHTTP interface {
	Do(method, url string, body []byte) (*Response, error)
}

// HTTP client
type DruidClient struct {
	HTTPClient *http.Client
	Auth       *Auth
}

func NewHTTPClient(client *http.Client, auth *Auth) DruidHTTP {
	newClient := &DruidClient{
		HTTPClient: client,
		Auth:       auth,
	}

	return newClient
}

// Auth mechanisms supported by Druid control plane to authenticate
// with druid clusters
type Auth struct {
	BasicAuth BasicAuth
}

// BasicAuth
type BasicAuth struct {
	UserName string
	Password string
}

// Response passed to controller
type Response struct {
	ResponseBody string
	StatusCode   int
}

// Do method to be used schema and tenant controller.
func (c *DruidClient) Do(Method, url string, body []byte) (*Response, error) {

	req, err := http.NewRequest(Method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	if c.Auth.BasicAuth != (BasicAuth{}) {
		req.SetBasicAuth(c.Auth.BasicAuth.UserName, c.Auth.BasicAuth.Password)
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &Response{ResponseBody: string(responseBody), StatusCode: resp.StatusCode}, nil
}
