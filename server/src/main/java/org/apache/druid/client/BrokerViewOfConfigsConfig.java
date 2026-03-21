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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Config for {@code druid.broker.configs}.
 */
public class BrokerViewOfConfigsConfig
{
  /**
   * If true (default), broker startup blocks until dynamic configs are fetched from the Coordinator.
   * If false, broker starts immediately using default configs and receives future updates via push.
   */
  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }
}
