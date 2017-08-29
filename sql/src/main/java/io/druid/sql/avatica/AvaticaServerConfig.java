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

package io.druid.sql.avatica;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

public class AvaticaServerConfig
{
  @JsonProperty
  public int maxConnections = 25;

  @JsonProperty
  public int maxStatementsPerConnection = 4;

  @JsonProperty
  public Period connectionIdleTimeout = new Period("PT5M");

  @JsonProperty
  public int maxRowsPerFrame = 100_000;

  public int getMaxConnections()
  {
    return maxConnections;
  }

  public int getMaxStatementsPerConnection()
  {
    return maxStatementsPerConnection;
  }

  public Period getConnectionIdleTimeout()
  {
    return connectionIdleTimeout;
  }

  public int getMaxRowsPerFrame()
  {
    return maxRowsPerFrame;
  }
}
