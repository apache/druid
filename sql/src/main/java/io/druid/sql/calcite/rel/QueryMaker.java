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

package io.druid.sql.calcite.rel;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.query.DataSource;
import io.druid.query.QueryDataSource;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.joda.time.DateTime;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class QueryMaker
{
  private final QuerySegmentWalker walker;
  private final PlannerConfig plannerConfig;

  public QueryMaker(
      final QuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    this.walker = walker;
    this.plannerConfig = plannerConfig;
  }

  public static Function<Row, Void> sinkFunction(final Sink sink)
  {
    return new Function<Row, Void>()
    {
      @Override
      public Void apply(final Row row)
      {
        try {
          sink.send(row);
          return null;
        }
        catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  public void accumulate(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final DruidQueryBuilder queryBuilder,
      final Function<Row, Void> sink
  )
  {
    if (dataSource instanceof QueryDataSource) {
      final GroupByQuery outerQuery = queryBuilder.toGroupByQuery(dataSource, sourceRowSignature);
      if (outerQuery == null) {
        // Bug in the planner rules. They shouldn't allow this to happen.
        throw new IllegalStateException("Can't use QueryDataSource without an outer groupBy query!");
      }

      executeGroupBy(queryBuilder, outerQuery, sink);
      return;
    }

    final TimeseriesQuery timeseriesQuery = queryBuilder.toTimeseriesQuery(dataSource, sourceRowSignature);
    if (timeseriesQuery != null) {
      executeTimeseries(queryBuilder, timeseriesQuery, sink);
      return;
    }

    final TopNQuery topNQuery = queryBuilder.toTopNQuery(
        dataSource,
        sourceRowSignature,
        plannerConfig.getMaxTopNLimit(),
        plannerConfig.isUseApproximateTopN()
    );
    if (topNQuery != null) {
      executeTopN(queryBuilder, topNQuery, sink);
      return;
    }

    final GroupByQuery groupByQuery = queryBuilder.toGroupByQuery(dataSource, sourceRowSignature);
    if (groupByQuery != null) {
      executeGroupBy(queryBuilder, groupByQuery, sink);
      return;
    }

    final SelectQuery selectQuery = queryBuilder.toSelectQuery(dataSource, sourceRowSignature);
    if (selectQuery != null) {
      executeSelect(queryBuilder, selectQuery, sink);
      return;
    }

    throw new IllegalStateException("WTF?! Cannot execute query even though we planned it?");
  }

  private void executeSelect(
      final DruidQueryBuilder queryBuilder,
      final SelectQuery baseQuery,
      final Function<Row, Void> sink
  )
  {
    Preconditions.checkState(queryBuilder.getGrouping() == null, "grouping must be null");

    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();
    final Row.RowBuilder rowBuilder = Row.newBuilder(fieldList.size());
    final Integer limit = queryBuilder.getLimitSpec() != null ? queryBuilder.getLimitSpec().getLimit() : null;

    // Loop through pages.
    final AtomicBoolean morePages = new AtomicBoolean(true);
    final AtomicReference<Map<String, Integer>> pagingIdentifiers = new AtomicReference<>();
    final AtomicLong rowsRead = new AtomicLong();

    while (morePages.get()) {
      final SelectQuery query = baseQuery.withPagingSpec(
          new PagingSpec(
              pagingIdentifiers.get(),
              plannerConfig.getSelectThreshold(),
              true
          )
      );

      Hook.QUERY_PLAN.run(query);

      morePages.set(false);
      final AtomicBoolean gotResult = new AtomicBoolean();

      query.run(walker, Maps.<String, Object>newHashMap()).accumulate(
          null,
          new Accumulator<Object, Result<SelectResultValue>>()
          {
            @Override
            public Object accumulate(final Object accumulated, final Result<SelectResultValue> result)
            {
              if (!gotResult.compareAndSet(false, true)) {
                throw new ISE("WTF?! Expected single result from Select query but got multiple!");
              }

              pagingIdentifiers.set(result.getValue().getPagingIdentifiers());

              for (EventHolder holder : result.getValue().getEvents()) {
                morePages.set(true);
                final Map<String, Object> map = holder.getEvent();
                for (RelDataTypeField field : fieldList) {
                  final String outputName = queryBuilder.getRowOrder().get(field.getIndex());
                  if (outputName.equals(Column.TIME_COLUMN_NAME)) {
                    rowBuilder.set(
                        field.getIndex(),
                        coerce(holder.getTimestamp().getMillis(), field.getType().getSqlTypeName())
                    );
                  } else {
                    rowBuilder.set(
                        field.getIndex(),
                        coerce(map.get(outputName), field.getType().getSqlTypeName())
                    );
                  }
                }
                if (limit == null || rowsRead.incrementAndGet() <= limit) {
                  sink.apply(rowBuilder.build());
                } else {
                  morePages.set(false);
                  break;
                }
                rowBuilder.reset();
              }

              return null;
            }
          }
      );
    }
  }

  private void executeTimeseries(
      final DruidQueryBuilder queryBuilder,
      final TimeseriesQuery query,
      final Function<Row, Void> sink
  )
  {
    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();
    final List<DimensionSpec> dimensions = queryBuilder.getGrouping().getDimensions();
    final String timeOutputName = dimensions.isEmpty() ? null : Iterables.getOnlyElement(dimensions).getOutputName();
    final Row.RowBuilder rowBuilder = Row.newBuilder(fieldList.size());

    Hook.QUERY_PLAN.run(query);

    query.run(walker, Maps.<String, Object>newHashMap()).accumulate(
        null,
        new Accumulator<Object, Result<TimeseriesResultValue>>()
        {
          @Override
          public Object accumulate(final Object accumulated, final Result<TimeseriesResultValue> result)
          {
            final Map<String, Object> row = result.getValue().getBaseObject();

            for (final RelDataTypeField field : fieldList) {
              final String outputName = queryBuilder.getRowOrder().get(field.getIndex());
              if (outputName.equals(timeOutputName)) {
                rowBuilder.set(field.getIndex(), coerce(result.getTimestamp(), field.getType().getSqlTypeName()));
              } else {
                rowBuilder.set(field.getIndex(), coerce(row.get(outputName), field.getType().getSqlTypeName()));
              }
            }

            sink.apply(rowBuilder.build());
            rowBuilder.reset();

            return null;
          }
        }
    );
  }

  private void executeTopN(
      final DruidQueryBuilder queryBuilder,
      final TopNQuery query,
      final Function<Row, Void> sink
  )
  {
    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();
    final Row.RowBuilder rowBuilder = Row.newBuilder(fieldList.size());

    Hook.QUERY_PLAN.run(query);

    query.run(walker, Maps.<String, Object>newHashMap()).accumulate(
        null,
        new Accumulator<Object, Result<TopNResultValue>>()
        {
          @Override
          public Object accumulate(final Object accumulated, final Result<TopNResultValue> result)
          {
            final List<DimensionAndMetricValueExtractor> values = result.getValue().getValue();

            for (DimensionAndMetricValueExtractor value : values) {
              for (final RelDataTypeField field : fieldList) {
                final String outputName = queryBuilder.getRowOrder().get(field.getIndex());
                rowBuilder.set(field.getIndex(), coerce(value.getMetric(outputName), field.getType().getSqlTypeName()));
              }

              sink.apply(rowBuilder.build());
              rowBuilder.reset();
            }

            return null;
          }
        }
    );
  }

  private void executeGroupBy(
      final DruidQueryBuilder queryBuilder,
      final GroupByQuery query,
      final Function<Row, Void> sink
  )
  {
    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();
    final Row.RowBuilder rowBuilder = Row.newBuilder(fieldList.size());

    Hook.QUERY_PLAN.run(query);

    query.run(walker, Maps.<String, Object>newHashMap()).accumulate(
        null,
        new Accumulator<Object, io.druid.data.input.Row>()
        {
          @Override
          public Object accumulate(final Object accumulated, final io.druid.data.input.Row row)
          {
            for (RelDataTypeField field : fieldList) {
              rowBuilder.set(
                  field.getIndex(),
                  coerce(
                      row.getRaw(queryBuilder.getRowOrder().get(field.getIndex())),
                      field.getType().getSqlTypeName()
                  )
              );
            }
            sink.apply(rowBuilder.build());
            rowBuilder.reset();

            return null;
          }
        }
    );
  }

  private static Object coerce(final Object value, final SqlTypeName sqlType)
  {
    final Object coercedValue;

    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      if (value == null || value instanceof String) {
        coercedValue = Strings.nullToEmpty((String) value);
      } else if (value instanceof NlsString) {
        coercedValue = ((NlsString) value).getValue();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (value == null) {
      coercedValue = null;
    } else if (sqlType == SqlTypeName.DATE) {
      final Long millis = (Long) coerce(value, SqlTypeName.TIMESTAMP);
      if (millis == null) {
        return null;
      } else {
        return new DateTime(millis.longValue()).dayOfMonth().roundFloorCopy().getMillis();
      }
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      if (value instanceof Number) {
        coercedValue = new DateTime(((Number) value).longValue()).getMillis();
      } else if (value instanceof String) {
        coercedValue = Long.parseLong((String) value);
      } else if (value instanceof Calendar) {
        coercedValue = ((Calendar) value).getTimeInMillis();
      } else if (value instanceof DateTime) {
        coercedValue = ((DateTime) value).getMillis();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.INTEGER) {
      if (value instanceof String) {
        coercedValue = Ints.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).intValue();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.BIGINT) {
      if (value instanceof String) {
        coercedValue = GuavaUtils.tryParseLong((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).longValue();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.FLOAT || sqlType == SqlTypeName.DOUBLE) {
      if (value instanceof String) {
        coercedValue = Doubles.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).doubleValue();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }

    return coercedValue;
  }
}
