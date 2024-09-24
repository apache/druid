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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.server.DruidNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unique identifier for a particular run of the controller server. This does not correspond to any specific
 * {@link Controller}; all controllers on the same server use the same ID.
 */
public class ControllerServerId
{
  private static final Pattern PATTERN = Pattern.compile("^(.+):(\\d+)$");

  private final String host;
  private final long epoch;
  private final String fullString;

  public ControllerServerId(final String host, final long epoch)
  {
    this.host = Preconditions.checkNotNull(host, "host");
    this.epoch = epoch;
    this.fullString = host + ':' + epoch;
  }

  @JsonCreator
  public static ControllerServerId fromString(final String s)
  {
    final Matcher matcher = PATTERN.matcher(s);
    if (matcher.matches()) {
      return new ControllerServerId(matcher.group(1), Long.parseLong(matcher.group(2)));
    } else {
      throw new IAE("Invalid controllerId[%s]", s);
    }
  }

  /**
   * Host and port, from {@link DruidNode#getHostAndPortToUse()}, of the controller server.
   */
  @JsonProperty
  public String getHost()
  {
    return host;
  }

  /**
   * Epoch of the controller server. Increased every time the server reboots. This can be used to determine if
   * the controller server being communicated with is the "same" controller server as one we know about with the
   * same hostname.
   */
  @JsonProperty
  public long getEpoch()
  {
    return epoch;
  }

  /**
   * Returns whether this controllerId replaces another one, i.e., if the host is the same and epoch is greater.
   */
  public boolean replaces(final ControllerServerId otherId)
  {
    return otherId.getHost().equals(host) && epoch > otherId.getEpoch();
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
    ControllerServerId that = (ControllerServerId) o;
    return fullString.equals(that.fullString);
  }

  @Override
  public int hashCode()
  {
    return fullString.hashCode();
  }

  @Override
  @JsonValue
  public String toString()
  {
    return fullString;
  }
}
