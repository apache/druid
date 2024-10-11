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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.spec.QuerySegmentSpec;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
@ExtensionPoint
public abstract class SingleDataSourceQuery<T> extends BaseQuery<T> implements Query<T>
{
  private final DataSource dataSource;

  public SingleDataSourceQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      Map<String, Object> context
  )
  {
    this(dataSource, querySegmentSpec, context, Granularities.ALL);
  }

  public SingleDataSourceQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      Map<String, Object> context,
      Granularity granularity
  )
  {
    super(querySegmentSpec, context, granularity);

    this.dataSource = dataSource;
  }

  @JsonProperty
  @Override
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @Override
  public QueryRunner<T> getRunner(QuerySegmentWalker walker)
  {
    return getQuerySegmentSpecForLookUp(this).lookup(this, walker);
  }

  @VisibleForTesting
  public static QuerySegmentSpec getQuerySegmentSpecForLookUp(SingleDataSourceQuery<?> query)
  {
    DataSource queryDataSource = query.getDataSource();
    return queryDataSource.getAnalysis()
                             .getBaseQuerySegmentSpec()
                             .orElseGet(query::getQuerySegmentSpec);
  }

  /**
   * Default implementation of {@link Query#getResultOrdering()} that uses {@link Ordering#natural()}.
   *
   * If your query result type T is not Comparable, you must override this method.
   */
  @Override
  @SuppressWarnings("unchecked") // assumes T is Comparable; see method javadoc
  public Ordering<T> getResultOrdering()
  {
    return (Ordering<T>) Ordering.natural();
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
    if(!super.equals(o)) {
      return false;
    }
    SingleDataSourceQuery<?> baseQuery = (SingleDataSourceQuery<?>) o;
    return Objects.equals(dataSource, baseQuery.dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), dataSource);
  }
}
