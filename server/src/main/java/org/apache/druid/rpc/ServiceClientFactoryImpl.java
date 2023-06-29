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

package org.apache.druid.rpc;

import org.apache.druid.java.util.http.client.HttpClient;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Production implementation of {@link ServiceClientFactory}.
 */
public class ServiceClientFactoryImpl implements ServiceClientFactory
{
  private final HttpClient httpClient;
  private final ScheduledExecutorService connectExec;

  public ServiceClientFactoryImpl(
      final HttpClient httpClient,
      final ScheduledExecutorService connectExec
  )
  {
    this.httpClient = httpClient;
    this.connectExec = connectExec;
  }

  @Override
  public ServiceClient makeClient(
      final String serviceName,
      final ServiceLocator serviceLocator,
      final ServiceRetryPolicy retryPolicy
  )
  {
    return new ServiceClientImpl(serviceName, httpClient, serviceLocator, retryPolicy, connectExec);
  }
}
