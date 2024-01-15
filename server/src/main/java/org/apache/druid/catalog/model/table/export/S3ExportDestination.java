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

package org.apache.druid.catalog.model.table.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class S3ExportDestination implements ExportDestination
{
  public static final String TYPE_KEY = "s3";

  private final String uri;
  private final String username;

  public S3ExportDestination(Map<String, String> properties)
  {
    this(properties.get("uri"), properties.get("username"));
  }

  @JsonCreator
  public S3ExportDestination(@JsonProperty("uri") String uri, @JsonProperty("username") String username)
  {
    this.uri = uri;
    this.username = username;
  }

  @JsonProperty("uri")
  public String getUri()
  {
    return uri;
  }

  @JsonProperty("username")
  public String getUsername()
  {
    return username;
  }

  @Override
  @JsonIgnore
  public String getDestinationName()
  {
    return TYPE_KEY;
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
    S3ExportDestination that = (S3ExportDestination) o;
    return Objects.equals(uri, that.uri) && Objects.equals(username, that.username);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uri, username);
  }
}
