/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class UnionAllQuery<T> extends BaseQuery<Result<T>>
{
  private final List<Query<T>> queries;
  private final boolean sortOnUnion;

  @JsonCreator
  public UnionAllQuery(
      @JsonProperty("queries") List<Query<T>> queries,
      @JsonProperty("sortOnUnion") boolean sortOnUnion,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(getFirstQueryWithValidation(queries), false, context);
    this.queries = queries;
    this.sortOnUnion = sortOnUnion;

    String prevQueryType = null;
    for (Query query : queries) {
      String queryType = query.getType();
      if (prevQueryType != null && !prevQueryType.equals(queryType)) {
        Preconditions.checkArgument(false, "The type of embedded queries should be all the same");
      }
      prevQueryType = queryType;
    }
  }

  @JsonProperty
  public List<Query<T>> getQueries()
  {
    return queries;
  }

  @JsonProperty
  public boolean isSortOnUnion()
  {
    return sortOnUnion;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return Query.UNION_ALL;
  }

  @Override
  public Query<Result<T>> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new UnionAllQuery(queries, sortOnUnion, contextOverride);
  }

  @Override
  public Query<Result<T>> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<Result<T>> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }
}
