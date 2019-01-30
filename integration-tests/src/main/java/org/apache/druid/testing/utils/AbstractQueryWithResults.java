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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class AbstractQueryWithResults<QueryType>
{
  private final QueryType query;
  private final List<Map<String, Object>> expectedResults;

  @JsonCreator
  public AbstractQueryWithResults(
      @JsonProperty("query") QueryType query,
      @JsonProperty("expectedResults") List<Map<String, Object>> expectedResults
  )
  {
    this.query = query;
    this.expectedResults = expectedResults;
  }

  @JsonProperty
  public QueryType getQuery()
  {
    return query;
  }

  @JsonProperty
  public List<Map<String, Object>> getExpectedResults()
  {
    return expectedResults;
  }

  @Override
  public String toString()
  {
    return "QueryWithResults{" +
           "query=" + query +
           ", expectedResults=" + expectedResults +
           '}';
  }
}
