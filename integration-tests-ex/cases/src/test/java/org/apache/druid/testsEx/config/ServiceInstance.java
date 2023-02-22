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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One instance of a Druid or third-party service running on
 * a host or in a container.
 */
public class ServiceInstance
{
  /**
   * Name of the Docker container. Used in Docker commands against
   * the container, such as starting and stopping.
   */
  private final String container;

  /**
   * Name of the host running the service as known to the cluster
   * (which many not be visible to the host running the test.)
   * Assumed to be {@code <service>} or @{code <service>-<tag>}
   * if not explicitly set.
   */
  private final String host;

  /**
   * Tag used to identify a service when there are multiple
   * instances. The host is assumed to be @{code<service>-<tag>} if
   * not explicitly set.
   */
  private final String tag;

  /**
   * The port exposed by the service on its host. May not be
   * visible to the test. Required.
   */

  private final int port;

  /**
   * The proxy port visible for the test for this service. Defaults
   * to the same as the @{code port}. Define only if Docker is configured
   * for port mapping other than identity.
   */
  private final int proxyPort;

  @JsonCreator
  public ServiceInstance(
      @JsonProperty("container") String container,
      @JsonProperty("host") String host,
      @JsonProperty("tag") String tag,
      @JsonProperty("port") int port,
      @JsonProperty("proxyPort") int proxyPort
  )
  {
    this.container = container;
    this.host = host;
    this.tag = tag;
    this.port = port;
    this.proxyPort = proxyPort;
  }

  @JsonProperty("container")
  @JsonInclude(Include.NON_NULL)
  public String container()
  {
    return container;
  }

  @JsonProperty("host")
  @JsonInclude(Include.NON_NULL)
  public String host()
  {
    return host;
  }

  @JsonProperty("tag")
  @JsonInclude(Include.NON_NULL)
  public String tag()
  {
    return tag;
  }

  @JsonProperty("port")
  @JsonInclude(Include.NON_DEFAULT)
  public int port()
  {
    return port;
  }

  @JsonProperty("proxyPort")
  @JsonInclude(Include.NON_DEFAULT)
  public int proxyPort()
  {
    return proxyPort;
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }
}
