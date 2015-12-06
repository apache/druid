/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.testing;

/**
 */
public interface IntegrationTestingConfig
{
  public String getCoordinatorHost();

  public String getIndexerHost();

  public String getRouterHost();

  public String getBrokerHost();

  public String getHistoricalHost();

  public String getMiddleManagerHost();

  public String getZookeeperHosts();

  public String getKafkaHost();

  public String getProperty(String prop);
}
