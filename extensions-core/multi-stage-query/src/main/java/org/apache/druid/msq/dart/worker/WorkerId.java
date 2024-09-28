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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Worker IDs, of the type returned by {@link ControllerQueryKernelConfig#getWorkerIds()}.
 *
 * Dart workerIds are strings of the form "scheme:host:port:queryId", like
 * "https:host1.example.com:8083:2f05528c-a882-4da5-8b7d-2ecafb7f3f4e".
 */
public class WorkerId
{
  private static final Pattern PATTERN = Pattern.compile("^(\\w+):(.+:\\d+):([a-z0-9-]+)$");

  private final String scheme;
  private final String hostAndPort;
  private final String queryId;
  private final String fullString;

  public WorkerId(final String scheme, final String hostAndPort, final String queryId)
  {
    this.scheme = Preconditions.checkNotNull(scheme, "scheme");
    this.hostAndPort = Preconditions.checkNotNull(hostAndPort, "hostAndPort");
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    this.fullString = Joiner.on(':').join(scheme, hostAndPort, queryId);
  }

  @JsonCreator
  public static WorkerId fromString(final String s)
  {
    if (s == null) {
      throw new IAE("Missing workerId");
    }

    final Matcher matcher = PATTERN.matcher(s);
    if (matcher.matches()) {
      return new WorkerId(matcher.group(1), matcher.group(2), matcher.group(3));
    } else {
      throw new IAE("Invalid workerId[%s]", s);
    }
  }

  /**
   * Create a worker ID, which is a URL.
   */
  public static WorkerId fromDruidNode(final DruidNode node, final String queryId)
  {
    return new WorkerId(
        node.getServiceScheme(),
        node.getHostAndPortToUse(),
        queryId
    );
  }

  /**
   * Create a worker ID, which is a URL.
   */
  public static WorkerId fromDruidServerMetadata(final DruidServerMetadata server, final String queryId)
  {
    return new WorkerId(
        server.getHostAndTlsPort() != null ? "https" : "http",
        server.getHost(),
        queryId
    );
  }

  public String getScheme()
  {
    return scheme;
  }

  public String getHostAndPort()
  {
    return hostAndPort;
  }

  public String getQueryId()
  {
    return queryId;
  }

  public URI toUri()
  {
    try {
      final String path = StringUtils.format(
          "%s/workers/%s",
          DartWorkerResource.PATH,
          StringUtils.urlEncode(queryId)
      );

      return new URI(scheme, hostAndPort, path, null, null);
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @JsonValue
  public String toString()
  {
    return fullString;
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
    WorkerId workerId = (WorkerId) o;
    return Objects.equals(fullString, workerId.fullString);
  }

  @Override
  public int hashCode()
  {
    return fullString.hashCode();
  }
}
