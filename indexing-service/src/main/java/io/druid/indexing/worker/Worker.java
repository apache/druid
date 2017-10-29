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

package io.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.guice.annotations.PublicApi;

/**
 * A container for worker metadata.
 */
@PublicApi
public class Worker
{
  private final String scheme;
  private final String host;
  private final String ip;
  private final int capacity;
  private final String version;

  @JsonCreator
  public Worker(
      @JsonProperty("scheme") String scheme,
      @JsonProperty("host") String host,
      @JsonProperty("ip") String ip,
      @JsonProperty("capacity") int capacity,
      @JsonProperty("version") String version
  )
  {
    this.scheme = scheme == null ? "http" : scheme; // needed for backwards compatibility with older workers (pre-#4270)
    this.host = host;
    this.ip = ip;
    this.capacity = capacity;
    this.version = version;
  }

  @JsonProperty
  public String getScheme()
  {
    return scheme;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public String getIp()
  {
    return ip;
  }

  @JsonProperty
  public int getCapacity()
  {
    return capacity;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
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

    Worker worker = (Worker) o;

    if (capacity != worker.capacity) {
      return false;
    }
    if (!scheme.equals(worker.scheme)) {
      return false;
    }
    if (!host.equals(worker.host)) {
      return false;
    }
    if (!ip.equals(worker.ip)) {
      return false;
    }
    return version.equals(worker.version);
  }

  @Override
  public int hashCode()
  {
    int result = scheme.hashCode();
    result = 31 * result + host.hashCode();
    result = 31 * result + ip.hashCode();
    result = 31 * result + capacity;
    result = 31 * result + version.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "Worker{" +
           "scheme='" + scheme + '\'' +
           ", host='" + host + '\'' +
           ", ip='" + ip + '\'' +
           ", capacity=" + capacity +
           ", version='" + version + '\'' +
           '}';
  }

}
