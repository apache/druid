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

package org.apache.druid.jdbc.http;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a SQL query request to be sent to Druid. Same model as the {@code SqlQuery} class, but we don't use that
 * here because the druid-jdbc-driver module wants to have minimal dependencies.
 */
public record SqlRequest(
    @JsonProperty("query") String query,
    @JsonProperty("resultFormat") String resultFormat,
    @JsonProperty("header") @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean header,
    @JsonProperty("typesHeader") @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean typesHeader,
    @JsonProperty("sqlTypesHeader") @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean sqlTypesHeader,
    @JsonProperty("context") @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Object> context,
    @JsonProperty("parameters") @JsonInclude(JsonInclude.Include.NON_EMPTY) List<SqlParameter> parameters
)
{
  public SqlRequest
  {
    Objects.requireNonNull(query, "query");
    resultFormat = resultFormat != null ? resultFormat : "array";
    context = context != null ? new HashMap<>(context) : new HashMap<>();
    parameters = parameters != null ? parameters : List.of();
  }

  /**
   * Creates a request in the form this driver always uses: "array" result format with all headers requested.
   */
  public static SqlRequest of(
      final String query,
      @Nullable final Map<String, Object> context,
      @Nullable final List<SqlParameter> parameters
  )
  {
    final Map<String, Object> mergedContext = context != null ? new HashMap<>(context) : new HashMap<>();

    // Need sqlStringifyArrays: false to properly read arrays.
    mergedContext.put("sqlStringifyArrays", false);

    return new SqlRequest(query, null, true, true, true, mergedContext, parameters);
  }
}
