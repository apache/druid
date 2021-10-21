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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
               IntStream.range(0, unionDataSource.getDataSources().size())
                         .mapToObj(i -> new Pair<Integer, DataSource>(i + 1, unionDataSource.getDataSources().get(i)))
                         .map(indexBaseDataSourcePair -> baseRunner.run(queryPlus.withQuery(Queries.withBaseDataSource(
                             query,
                             indexBaseDataSourcePair.rhs
                         ).withSubQueryId(generateSubqueryId(
                             query.getSubQueryId(),
                             findNestingLevel(analysis.getDataSource(), analysis.getBaseDataSource()),
                             indexBaseDataSourcePair.lhs
                         ))))).collect(
                             Collectors.toList())
            )
        );
      }
    } else {
      // Not a union of tables. Do nothing special.
      return baseRunner.run(queryPlus, responseContext);
    }
  }
  /**
   * Creates a child subquery Id from the parent (sub)query as follows
   *  If the parent (sub)query Id is not null, i.e. it is a top level query, it simply returns the orderNumber
   *  Else it appends the orderNumber to the parent (sub)query Id with '-' as separator
   *
   * @param parentSubqueryId The subquery Id of the parent query which is generating this subquery
   * @param nesting The level under which the base datasource is present inside the original datasource
   * @param orderNumber Position of the generated subquery at the same level
   * @return Subquery Id which needs to be populated
   */
  private String generateSubqueryId(String parentSubqueryId, int nesting, int orderNumber)
  {
    List<String> arr = new ArrayList<>();
    if (!StringUtils.isEmpty(parentSubqueryId)) {
      arr.add(parentSubqueryId);
    }
    arr.addAll(Collections.nCopies(nesting, "1"));
    arr.add(Integer.toString(orderNumber));

    return String.join("-", arr);
  }

  /**
   * Finds the nesting level of the base datasource inside the datasource object. This method walks the datasource
   * in a fashion which is similar to {@link DataSourceAnalysis#forDataSource(DataSource)}
   * @param dataSource The datasource object present in the query
   * @param baseUnionDataSource The base union datasource found from the datasource analysis
   * @return Nesting level of the base datasource
   */
  private int findNestingLevel(DataSource dataSource, DataSource baseUnionDataSource) {
    int nesting = 0;
    while (dataSource instanceof QueryDataSource) {
      ++nesting;
      dataSource = ((QueryDataSource) dataSource).getQuery().getDataSource();
    }
    while (dataSource instanceof JoinDataSource) {
      ++nesting;
      dataSource = ((JoinDataSource) dataSource).getLeft();
    }

    //noinspection ObjectEquality
    if(dataSource != baseUnionDataSource) {
      // We donot expect to reach here since the datasource analysis uses the same traversal to fetch the base union datasource
      return 0;
    }
    ++nesting;
    return nesting;
  }
}
