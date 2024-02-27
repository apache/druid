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

package org.apache.druid.msq.test;

import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.sql.calcite.QueryTestRunner;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Custom {@link QueryTestRunner.QueryVerifyStep} for the MSQ run queries. Since all the query types are not supported
 * therefore we skip the {@link QueryTestRunner.VerifyNativeQueries} check for some query types
 */
public class VerifyMSQSupportedNativeQueriesPredicate implements Predicate<List<Query<?>>>
{
  @Override
  public boolean test(List<Query<?>> expectedQueries)
  {
    final boolean unsupportedQuery = expectedQueries.stream().anyMatch(this::isUnsupportedQuery);
    return !unsupportedQuery;
  }

  private boolean isUnsupportedQuery(Query<?> query)
  {
    if (!Objects.equals(query.getType(), Query.GROUP_BY) && !Objects.equals(query.getType(), Query.SCAN)) {
      return true;
    }
    DataSource dataSource = query.getDataSource();
    return isUnsupportedDataSource(dataSource);
  }

  private boolean isUnsupportedDataSource(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      final Query<?> subQuery = ((QueryDataSource) dataSource).getQuery();
      return isUnsupportedQuery(subQuery);
    } else {
      return dataSource.getChildren().stream().anyMatch(this::isUnsupportedDataSource);
    }
  }
}
