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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.com.google.common.base.Function;
import org.apache.druid.com.google.common.base.Functions;
import org.apache.druid.com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.List;
import java.util.Map;

public class ScanQueryQueryToolChest extends QueryToolChest<ScanResultValue, ScanQuery>
{
  private static final TypeReference<ScanResultValue> TYPE_REFERENCE = new TypeReference<ScanResultValue>()
  {
  };

  private final ScanQueryConfig scanQueryConfig;
  private final GenericQueryMetricsFactory queryMetricsFactory;

  @Inject
  public ScanQueryQueryToolChest(
      final ScanQueryConfig scanQueryConfig,
      final GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.scanQueryConfig = scanQueryConfig;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<ScanResultValue> mergeResults(final QueryRunner<ScanResultValue> runner)
  {
    return (queryPlus, responseContext) -> {
      // Ensure "legacy" is a non-null value, such that all other nodes this query is forwarded to will treat it
      // the same way, even if they have different default legacy values.
      //
      // Also, remove "offset" and add it to the "limit" (we won't push the offset down, just apply it here, at the
      // merge at the top of the stack).
      final ScanQuery originalQuery = ((ScanQuery) (queryPlus.getQuery()));

      final long newLimit;
      if (!originalQuery.isLimited()) {
        // Unlimited stays unlimited.
        newLimit = Long.MAX_VALUE;
      } else if (originalQuery.getScanRowsLimit() > Long.MAX_VALUE - originalQuery.getScanRowsOffset()) {
        throw new ISE(
            "Cannot apply limit[%d] with offset[%d] due to overflow",
            originalQuery.getScanRowsLimit(),
            originalQuery.getScanRowsOffset()
        );
      } else {
        newLimit = originalQuery.getScanRowsLimit() + originalQuery.getScanRowsOffset();
      }

      final ScanQuery queryToRun = originalQuery.withNonNullLegacy(scanQueryConfig)
                                                .withOffset(0)
                                                .withLimit(newLimit);

      final Sequence<ScanResultValue> results;

      if (!queryToRun.isLimited()) {
        results = runner.run(queryPlus.withQuery(queryToRun), responseContext);
      } else {
        results = new BaseSequence<>(
            new BaseSequence.IteratorMaker<ScanResultValue, ScanQueryLimitRowIterator>()
            {
              @Override
              public ScanQueryLimitRowIterator make()
              {
                return new ScanQueryLimitRowIterator(runner, queryPlus.withQuery(queryToRun), responseContext);
              }

              @Override
              public void cleanup(ScanQueryLimitRowIterator iterFromMake)
              {
                CloseQuietly.close(iterFromMake);
              }
            });
      }

      if (originalQuery.getScanRowsOffset() > 0) {
        return new ScanQueryOffsetSequence(results, originalQuery.getScanRowsOffset());
      } else {
        return results;
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(ScanQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<ScanResultValue, ScanResultValue> makePreComputeManipulatorFn(
      ScanQuery query,
      MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<ScanResultValue> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<ScanResultValue> preMergeQueryDecoration(final QueryRunner<ScanResultValue> runner)
  {
    return (queryPlus, responseContext) -> {
      return runner.run(queryPlus, responseContext);
    };
  }

  @Override
  public RowSignature resultArraySignature(final ScanQuery query)
  {
    if (query.getColumns() == null || query.getColumns().isEmpty()) {
      // Note: if no specific list of columns is provided, then since we can't predict what columns will come back, we
      // unfortunately can't do array-based results. In this case, there is a major difference between standard and
      // array-based results: the standard results will detect and return _all_ columns, whereas the array-based results
      // will include none of them.
      return RowSignature.empty();
    } else {
      final RowSignature.Builder builder = RowSignature.builder();

      if (query.withNonNullLegacy(scanQueryConfig).isLegacy()) {
        builder.add(ScanQueryEngine.LEGACY_TIMESTAMP_KEY, null);
      }

      for (String columnName : query.getColumns()) {
        // With the Scan query we only know the columnType for virtual columns. Let's report those, at least.
        final ValueType columnType;

        final VirtualColumn virtualColumn = query.getVirtualColumns().getVirtualColumn(columnName);
        if (virtualColumn != null) {
          columnType = virtualColumn.capabilities(columnName).getType();
        } else {
          // Unknown type. In the future, it would be nice to have a way to fill these in.
          columnType = null;
        }

        builder.add(columnName, columnType);
      }

      return builder.build();
    }
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(final ScanQuery query, final Sequence<ScanResultValue> resultSequence)
  {
    final List<String> fields = resultArraySignature(query).getColumnNames();
    final Function<?, Object[]> mapper;

    switch (query.getResultFormat()) {
      case RESULT_FORMAT_LIST:
        mapper = (Map<String, Object> row) -> {
          final Object[] rowArray = new Object[fields.size()];

          for (int i = 0; i < fields.size(); i++) {
            rowArray[i] = row.get(fields.get(i));
          }

          return rowArray;
        };
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        mapper = (List<Object> row) -> {
          if (row.size() == fields.size()) {
            return row.toArray();
          } else if (fields.isEmpty()) {
            return new Object[0];
          } else {
            // Uh oh... mismatch in expected and actual field count. I don't think this should happen, so let's
            // throw an exception. If this really does happen, and there's a good reason for it, then we should remap
            // the result row here.
            throw new ISE("Mismatch in expected[%d] vs actual[%s] field count", fields.size(), row.size());
          }
        };
        break;
      default:
        throw new UOE("Unsupported resultFormat for array-based results: %s", query.getResultFormat());
    }

    return resultSequence.flatMap(
        result -> {
          // Generics? Where we're going, we don't need generics.
          final List rows = (List) result.getEvents();
          final Iterable arrays = Iterables.transform(rows, (Function) mapper);
          return Sequences.simple(arrays);
        }
    );
  }
}
