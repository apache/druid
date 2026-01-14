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

package org.apache.druid.testing.embedded;

import com.google.common.net.HostAndPort;
import org.apache.druid.java.util.common.IAE;
import org.apache.http.client.utils.URIBuilder;
import org.testcontainers.DockerClientFactory;

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

/**
 * Hostname to be used by embedded services, both Druid and external.
 * The default value is {@code localhost}.
 * <p>
 * When running Druid containers, {@code localhost} cannot be used to connect to
 * services since {@code localhost} refers to the container and not the host
 * machine.
 */
public class EmbeddedHostname
{
  private static final String DOCKER_HOST_INTERNAL = "host.docker.internal";
  private static final EmbeddedHostname LOCALHOST = new EmbeddedHostname("localhost");

  private final String hostname;

  private EmbeddedHostname(String hostname)
  {
    this.hostname = hostname;
  }

  public static EmbeddedHostname localhost()
  {
    return LOCALHOST;
  }

  /**
   * Special hostname for the Docker host that is reachable from both Docker
   * containers and the JVM running the tests.
   * <p>
   * Uses {@code host.docker.internal} which communication patterns where:
   * <ul>
   *   <li>Embedded JVM services can reach Docker containers via host port bindings</li>
   *   <li>Docker containers can reach the host (and other containers via host ports)</li>
   * </ul>
   */
  public static EmbeddedHostname containerFriendly()
  {
    // Prefer host.docker.internal if available (Docker Desktop)
    try {
      InetAddress.getByName(DOCKER_HOST_INTERNAL);
      return new EmbeddedHostname(DOCKER_HOST_INTERNAL);
    }
    catch (UnknownHostException e) {
      // Fallback: use testcontainers' host IP detection
      // Returns Docker bridge gateway on Linux (e.g., 172.17.0.1)
      String hostIp = DockerClientFactory.instance().dockerHostIpAddress();
      return new EmbeddedHostname(hostIp);
    }
  }

  /**
   * Replaces {@code localhost} or {@code 127.0.0.1} in the given connectUri
   * with {@link #hostname}.
   *
   * @param connectUri Syntactically valid connect URI, complete with a scheme, host and port.
   */
  public String useInUri(String connectUri)
  {
    try {
      final URIBuilder uri = new URIBuilder(connectUri);
      validateLocalhost(uri.getHost(), connectUri);
      uri.setHost(hostname);
      return uri.build().toString();
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Replaces {@code localhost} or {@code 127.0.0.1} in the given hostAndPort
   * with {@link #hostname}.
   */
  public String useInHostAndPort(String hostAndPort)
  {
    final HostAndPort parsedHostAndPort = HostAndPort.fromString(hostAndPort);
    validateLocalhost(parsedHostAndPort.getHost(), hostAndPort);
    return HostAndPort.fromParts(hostname, parsedHostAndPort.getPort()).toString();
  }

  private static void validateLocalhost(String host, String connectUri)
  {
    if (!"localhost".equals(host) && !"127.0.0.1".equals(host)) {
      throw new IAE(
          "Connect URI[%s] must contain 'localhost' or '127.0.0.1' to be reachable.",
          connectUri
      );
    }
  }

  @Override
  public String toString()
  {
    return hostname;
  }
}
