/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.druid.emitter.ganglia;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class GangliaEmitterConfig
{

  @JsonProperty
  final private String hostname;
  @JsonProperty
  final private int port;

  @JsonCreator
  public GangliaEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port
  ) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GangliaEmitterConfig that = (GangliaEmitterConfig) o;

    if (port != that.port) {
      return false;
    }
    return hostname != null ? hostname.equals(that.hostname) : that.hostname == null;

  }

  @Override
  public int hashCode()
  {
    int result = hostname != null ? hostname.hashCode() : 0;
    result = 31 * result + port;
    return result;
  }

  @JsonProperty
  public String getHostname()
  {
    return hostname;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }
}
