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

package org.apache.druid.data.input.schemarepo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.schemarepo.client.Avro1124RESTRepositoryClient;

import java.util.Objects;

public class Avro1124RESTRepositoryClientWrapper extends Avro1124RESTRepositoryClient
{
  private final String url;

  public Avro1124RESTRepositoryClientWrapper(
      @JsonProperty("url") String url
  )
  {
    super(url);
    this.url = url;
  }

  @JsonIgnore
  @Override
  public String getStatus()
  {
    return super.getStatus();
  }

  @JsonProperty
  public String getUrl()
  {
    return url;
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

    Avro1124RESTRepositoryClientWrapper that = (Avro1124RESTRepositoryClientWrapper) o;

    return Objects.equals(url, that.url);
  }

  @Override
  public int hashCode()
  {
    return url != null ? url.hashCode() : 0;
  }
}
