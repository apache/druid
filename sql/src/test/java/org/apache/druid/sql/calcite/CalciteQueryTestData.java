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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.Query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A data class that holds data for {@link CalciteQueryTest}
 */
public class CalciteQueryTestData
{
  /**
   * A comment that describes what the test is doing
   */
  @Nonnull
  public final String testName;

  /**
   * The SQL query under test.
   */
  @Nonnull
  public final String sql;

  /**
   * The native query that this should transform to. If the SQL query generates many native queries, provide them
   * as a list. To generate the native query use `EXPLAIN PLAN FOR` for the same query in a druid cluster. If the Druid
   * cluster produces output that does not look like a native query, you can still see the native query by enabling
   * request logging on the broker. https://druid.apache.org/docs/latest/configuration/index.html#request-logging
   *
   * Set to null if the test does not need to test the shape of the native query.
   */
  @Nullable
  public final List<Query> expectedQueries;

  /**
   * The expected output of the query.
   *
   * Set to an empty list if the expected result is empty.
   */
  @Nonnull
  public final List<Object[]> expectedResults;

  /**
   * A link to the apache issue if it is expected that this query will not succeed.
   */
  @Nullable
  public final String apacheIssue;

  /**
   * The expected exception class to be thrown. Null if no exception is expected.
   */
  @Nullable
  public final Class expectedException;

  /**
   * Whether the query can be vectorized. By default, this is false (ie. it is assumed that the query
   * can be vectorized).
   */
  public final boolean cannotVectorize;

  @JsonCreator
  public CalciteQueryTestData(
      @JsonProperty("testName") @Nonnull String testName,
      @JsonProperty("sql") @Nonnull String sql,
      @JsonProperty("expectedQueries") @Nullable List<Query> expectedQueries,
      @JsonProperty("expectedResults") @Nonnull List<Object[]> expectedResults,
      @JsonProperty("apacheIssue") @Nullable String apacheIssue,
      @JsonProperty("expectedException") @Nullable Class expectedException,
      @JsonProperty("cannotVectorize") @Nullable Boolean cannotVectorize
  )
  {
    this.testName = testName;
    this.sql = sql;
    this.expectedQueries = expectedQueries;
    this.expectedResults = expectedResults;
    this.apacheIssue = apacheIssue;
    this.expectedException = expectedException;
    this.cannotVectorize = cannotVectorize == null ? false : cannotVectorize;
  }

  @Override
  public String toString()
  {
    StringBuilder resultBuilder = new StringBuilder("test - ").append(testName);
    if (expectedException != null) {
      resultBuilder.append(" withExpectedException=").append(expectedException.getName());
    }
    if (apacheIssue != null) {
      resultBuilder.append(" withKnownIssue=").append(apacheIssue);
    }
    return resultBuilder.toString();
  }
}
