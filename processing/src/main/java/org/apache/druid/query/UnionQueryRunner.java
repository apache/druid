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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public UnionQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    Query<T> query = queryPlus.getQuery();

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (analysis.isConcreteTableBased() && analysis.getBaseUnionDataSource().isPresent()) {
      // Union of tables.

      final UnionDataSource unionDataSource = analysis.getBaseUnionDataSource().get();

      if (unionDataSource.getDataSources().isEmpty()) {
        // Shouldn't happen, because UnionDataSource doesn't allow empty unions.
        throw new ISE("Unexpectedly received empty union");
      } else if (unionDataSource.getDataSources().size() == 1) {
        // Single table. Run as a normal query.
        return baseRunner.run(
            queryPlus.withQuery(
                Queries.withBaseDataSource(
                    query,
                    Iterables.getOnlyElement(unionDataSource.getDataSources())
                )
            ),
            responseContext
        );
      } else {
        // Split up the tables and merge their results.
        return new MergeSequence<>(
            query.getResultOrdering(),
            Sequences.simple(
                Lists.transform(
                    IntStream.range(0, unionDataSource.getDataSources().size())
                             .mapToObj(i -> new Pair<>(unionDataSource.getDataSources().get(i), i + 1))
                             .collect(Collectors.toList()),
                    (Function<Pair<TableDataSource, Integer>, Sequence<T>>) singleSourceWithIndex ->
                        baseRunner.run(
                            queryPlus.withQuery(
                                Queries.withBaseDataSource(query, singleSourceWithIndex.lhs)
                                       // assign the subqueryId. this will be used to validate that every query servers
                                       // have responded per subquery in RetryQueryRunner
                                       .withSubQueryId(generateSubqueryId(
                                           query.getSubQueryId(),
                                           singleSourceWithIndex.lhs.getName(),
                                           singleSourceWithIndex.rhs
                                       ))
                            ),
                            responseContext
                        )
                )
            )

        );
      }
    } else {
      // Not a union of tables. Do nothing special.
      return baseRunner.run(queryPlus, responseContext);
    }
  }

  /**
   * Appends and returns the name and the position of the individual datasource in the union with the parent query id
   * if preseent
   *
   * @param parentSubqueryId The subquery Id of the parent query which is generating this subquery
   * @param dataSourceName   Name of the datasource for which the UnionRunner is running
   * @param dataSourceIndex  Position of the datasource for which the UnionRunner is running
   * @return Subquery Id which needs to be populated
   */
  private String generateSubqueryId(String parentSubqueryId, String dataSourceName, int dataSourceIndex)
  {
    String dataSourceNameIndex = dataSourceName + "." + dataSourceIndex;
    if (StringUtils.isEmpty(parentSubqueryId)) {
      return dataSourceNameIndex;
    }
    return parentSubqueryId + "." + dataSourceNameIndex;
  }
}
