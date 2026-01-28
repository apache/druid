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

package org.apache.druid.query.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Client representation of {@link org.apache.druid.sql.http.SqlParameter}. This is effectively a lightweight POJO class
 * for use by clients that excludes Calcite dependencies and server-side logic from the Broker.
 */
public class ClientSqlParameter
{
  @JsonProperty
  private final String type;

  @JsonProperty
  private final Object value;

  @JsonCreator
  public ClientSqlParameter(
      @JsonProperty("type") String type,
      @JsonProperty("value") Object value
  )
  {
    this.type = type;
    this.value = value;
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
    ClientSqlParameter that = (ClientSqlParameter) o;
    return Objects.equals(type, that.type) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, value);
  }

  @Override
  public String toString()
  {
    return "ClientSqlParameter{" +
           "type='" + type + '\'' +
           ", value=" + value +
           '}';
  }
}
