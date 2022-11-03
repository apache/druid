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

/**
 * Factory for creating {@link ServiceClient}.
 */
public interface ServiceClientFactory
{
  /**
   * Creates a client for a particular service.
   *
   * @param serviceName    name of the service, which is used in log messages and exceptions.
   * @param serviceLocator service locator. This is not owned by the returned client, and should be closed
   *                       separately when you are done with it.
   * @param retryPolicy    retry policy
   */
  ServiceClient makeClient(String serviceName, ServiceLocator serviceLocator, ServiceRetryPolicy retryPolicy);
}
