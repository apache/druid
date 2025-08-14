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

package org.apache.druid.msq.exec;

import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.RestrictedInputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Static utility functions for {@link DataServerQueryHandler} implementations.
 */
public class DataServerQueryHandlerUtils
{
  private DataServerQueryHandlerUtils()
  {
    // No instantiation.
  }

  /**
   * Performs necessary transforms to a query destined for data servers. Does not update the list of segments; callers
   * should do this themselves using {@link Queries#withSpecificSegments(Query, List)}.
   *
   * @param query          the query
   * @param dataSourceName datasource name
   */
  public static <R, T extends Query<R>> Query<R> prepareQuery(
      final T query,
      final int inputNumber,
      final String dataSourceName
  )
  {
    // MSQ changes the datasource to an inputNumber datasource. This needs to be changed back for data servers
    // to understand.
    return query.withDataSource(transformDatasource(query.getDataSource(), inputNumber, dataSourceName));
  }

  /**
   * Transforms {@link InputNumberDataSource} and {@link RestrictedInputNumberDataSource}, which are only understood
   * by MSQ tasks, back into {@link TableDataSource} and {@link RestrictedDataSource} recursivly.
   */
  static DataSource transformDatasource(
      final DataSource dataSource,
      final int inputNumber,
      final String dataSourceName
  )
  {
    if (dataSource instanceof InputNumberDataSource) {
      InputNumberDataSource numberDataSource = (InputNumberDataSource) dataSource;
      if (numberDataSource.getInputNumber() == inputNumber) {
        return new TableDataSource(dataSourceName);
      } else {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.UNSUPPORTED)
                            .build(
                                "Cannot handle stage with multiple sources while querying realtime data. "
                                + "If using broadcast joins, try setting[%s] to[%s] in your query context.",
                                PlannerContext.CTX_SQL_JOIN_ALGORITHM,
                                JoinAlgorithm.SORT_MERGE.toString()
                            );
      }
    } else if (dataSource instanceof RestrictedInputNumberDataSource) {
      RestrictedInputNumberDataSource restrictedDatasource = (RestrictedInputNumberDataSource) dataSource;
      if (restrictedDatasource.getInputNumber() == inputNumber) {
        return RestrictedDataSource.create(new TableDataSource(dataSourceName), restrictedDatasource.getPolicy());
      } else {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.UNSUPPORTED)
                            .build(
                                "Cannot handle stage with multiple sources while querying realtime data. "
                                + "If using broadcast joins, try setting[%s] to[%s] in your query context.",
                                PlannerContext.CTX_SQL_JOIN_ALGORITHM,
                                JoinAlgorithm.SORT_MERGE.toString()
                            );
      }
    } else {
      List<DataSource> transformed = dataSource.getChildren()
                                               .stream()
                                               .map(ds -> transformDatasource(ds, inputNumber, dataSourceName))
                                               .collect(Collectors.toList());
      return dataSource.withChildren(transformed);
    }
  }

  /**
   * Given results from {@link DataServerClient#run}, returns a {@link Yielder} that applies the provided
   * mapping function and increments the row count on the provided {@link ChannelCounters}.
   */
  public static <RowType, QueryType> Yielder<RowType> createYielder(
      final Sequence<QueryType> sequence,
      final Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      final ChannelCounters channelCounters
  )
  {
    return Yielders.each(
        mappingFunction.apply(sequence)
                       .map(row -> {
                         channelCounters.incrementRowCount();
                         return row;
                       })
    );
  }

  /**
   * Retreives the list of missing segments from the response context.
   */
  public static List<SegmentDescriptor> getMissingSegments(final ResponseContext responseContext)
  {
    List<SegmentDescriptor> missingSegments = responseContext.getMissingSegments();
    if (missingSegments == null) {
      return Collections.emptyList();
    }
    return missingSegments;
  }
}
