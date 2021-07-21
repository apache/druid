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

package org.apache.druid.spark.configuration

object DruidConfigurationKeys {
  // Druid Client Configs
  val brokerPrefix: String = "broker"
  val brokerHostKey: String = "host"
  val brokerPortKey: String = "port"
  val numRetriesKey: String = "numRetries"
  val retryWaitSecondsKey: String = "retryWaitSeconds"
  val timeoutMillisecondsKey: String = "timeoutMilliseconds"
  private[spark] val brokerHostDefaultKey: (String, String) = (brokerHostKey, "localhost")
  private[spark] val brokerPortDefaultKey: (String, Int) = (brokerPortKey, 8082)
  private[spark] val numRetriesDefaultKey: (String, Int) = (numRetriesKey, 5)
  private[spark] val retryWaitSecondsDefaultKey: (String, Int) = (retryWaitSecondsKey, 5)
  private[spark] val timeoutMillisecondsDefaultKey: (String, Int) = (timeoutMillisecondsKey, 300000)
}
