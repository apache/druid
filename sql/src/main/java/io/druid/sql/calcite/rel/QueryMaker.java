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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
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
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
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

  public Sequence<Object[]> runQuery(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final DruidQueryBuilder queryBuilder
  )
  {
    if (dataSource instanceof QueryDataSource) {
      final GroupByQuery outerQuery = queryBuilder.toGroupByQuery(dataSource, sourceRowSignature);
      if (outerQuery == null) {
        // Bug in the planner rules. They shouldn't allow this to happen.
        throw new IllegalStateException("Can't use QueryDataSource without an outer groupBy query!");
      }

      return executeGroupBy(queryBuilder, outerQuery);
    }

    final TimeseriesQuery timeseriesQuery = queryBuilder.toTimeseriesQuery(dataSource, sourceRowSignature);
    if (timeseriesQuery != null) {
      return executeTimeseries(queryBuilder, timeseriesQuery);
    }

    final TopNQuery topNQuery = queryBuilder.toTopNQuery(
        dataSource,
        sourceRowSignature,
        plannerConfig.getMaxTopNLimit(),
        plannerConfig.isUseApproximateTopN()
    );
    if (topNQuery != null) {
      return executeTopN(queryBuilder, topNQuery);
    }

    final GroupByQuery groupByQuery = queryBuilder.toGroupByQuery(dataSource, sourceRowSignature);
    if (groupByQuery != null) {
      return executeGroupBy(queryBuilder, groupByQuery);
    }

    final SelectQuery selectQuery = queryBuilder.toSelectQuery(dataSource, sourceRowSignature);
    if (selectQuery != null) {
      return executeSelect(queryBuilder, selectQuery);
    }

    throw new IllegalStateException("WTF?! Cannot execute query even though we planned it?");
  }

  private Sequence<Object[]> executeSelect(
      final DruidQueryBuilder queryBuilder,
      final SelectQuery baseQuery
  )
  {
    Preconditions.checkState(queryBuilder.getGrouping() == null, "grouping must be null");

    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();
    final Integer limit = queryBuilder.getLimitSpec() != null ? queryBuilder.getLimitSpec().getLimit() : null;

    // Select is paginated, we need to make multiple queries.
    final Sequence<Sequence<Object[]>> sequenceOfSequences = Sequences.simple(
        new Iterable<Sequence<Object[]>>()
        {
          @Override
          public Iterator<Sequence<Object[]>> iterator()
          {
            final AtomicBoolean morePages = new AtomicBoolean(true);
            final AtomicReference<Map<String, Integer>> pagingIdentifiers = new AtomicReference<>();
            final AtomicLong rowsRead = new AtomicLong();

            // Each Sequence<Object[]> is one page.
            return new Iterator<Sequence<Object[]>>()
            {
              @Override
              public boolean hasNext()
              {
                return morePages.get();
              }

              @Override
              public Sequence<Object[]> next()
              {
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

                return Sequences.concat(
                    Sequences.map(
                        query.run(walker, Maps.<String, Object>newHashMap()),
                        new Function<Result<SelectResultValue>, Sequence<Object[]>>()
                        {
                          @Override
                          public Sequence<Object[]> apply(final Result<SelectResultValue> result)
                          {
                            if (!gotResult.compareAndSet(false, true)) {
                              throw new ISE("WTF?! Expected single result from Select query but got multiple!");
                            }

                            pagingIdentifiers.set(result.getValue().getPagingIdentifiers());
                            final List<Object[]> retVals = new ArrayList<>();

                            for (EventHolder holder : result.getValue().getEvents()) {
                              morePages.set(true);
                              final Map<String, Object> map = holder.getEvent();
                              final Object[] retVal = new Object[fieldList.size()];
                              for (RelDataTypeField field : fieldList) {
                                final String outputName = queryBuilder.getRowOrder().get(field.getIndex());
                                if (outputName.equals(Column.TIME_COLUMN_NAME)) {
                                  retVal[field.getIndex()] = coerce(
                                      holder.getTimestamp().getMillis(),
                                      field.getType().getSqlTypeName()
                                  );
                                } else {
                                  retVal[field.getIndex()] = coerce(
                                      map.get(outputName),
                                      field.getType().getSqlTypeName()
                                  );
                                }
                              }
                              if (limit == null || rowsRead.incrementAndGet() <= limit) {
                                retVals.add(retVal);
                              } else {
                                morePages.set(false);
                                return Sequences.simple(retVals);
                              }
                            }

                            return Sequences.simple(retVals);
                          }
                        }
                    )
                );
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );

    return Sequences.concat(sequenceOfSequences);
  }

  private Sequence<Object[]> executeTimeseries(
      final DruidQueryBuilder queryBuilder,
      final TimeseriesQuery query
  )
  {
    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();
    final List<DimensionSpec> dimensions = queryBuilder.getGrouping().getDimensions();
    final String timeOutputName = dimensions.isEmpty() ? null : Iterables.getOnlyElement(dimensions).getOutputName();

    Hook.QUERY_PLAN.run(query);

    return Sequences.map(
        query.run(walker, Maps.<String, Object>newHashMap()),
        new Function<Result<TimeseriesResultValue>, Object[]>()
        {
          @Override
          public Object[] apply(final Result<TimeseriesResultValue> result)
          {
            final Map<String, Object> row = result.getValue().getBaseObject();
            final Object[] retVal = new Object[fieldList.size()];

            for (final RelDataTypeField field : fieldList) {
              final String outputName = queryBuilder.getRowOrder().get(field.getIndex());
              if (outputName.equals(timeOutputName)) {
                retVal[field.getIndex()] = coerce(result.getTimestamp(), field.getType().getSqlTypeName());
              } else {
                retVal[field.getIndex()] = coerce(row.get(outputName), field.getType().getSqlTypeName());
              }
            }

            return retVal;
          }
        }
    );
  }

  private Sequence<Object[]> executeTopN(
      final DruidQueryBuilder queryBuilder,
      final TopNQuery query
  )
  {
    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();

    Hook.QUERY_PLAN.run(query);

    return Sequences.concat(
        Sequences.map(
            query.run(walker, Maps.<String, Object>newHashMap()),
            new Function<Result<TopNResultValue>, Sequence<Object[]>>()
            {
              @Override
              public Sequence<Object[]> apply(final Result<TopNResultValue> result)
              {
                final List<DimensionAndMetricValueExtractor> rows = result.getValue().getValue();
                final List<Object[]> retVals = new ArrayList<>(rows.size());

                for (DimensionAndMetricValueExtractor row : rows) {
                  final Object[] retVal = new Object[fieldList.size()];
                  for (final RelDataTypeField field : fieldList) {
                    final String outputName = queryBuilder.getRowOrder().get(field.getIndex());
                    retVal[field.getIndex()] = coerce(row.getMetric(outputName), field.getType().getSqlTypeName());
                  }

                  retVals.add(retVal);
                }

                return Sequences.simple(retVals);
              }
            }
        )
    );
  }

  private Sequence<Object[]> executeGroupBy(
      final DruidQueryBuilder queryBuilder,
      final GroupByQuery query
  )
  {
    final List<RelDataTypeField> fieldList = queryBuilder.getRowType().getFieldList();

    Hook.QUERY_PLAN.run(query);

    return Sequences.map(
        query.run(walker, Maps.<String, Object>newHashMap()),
        new Function<io.druid.data.input.Row, Object[]>()
        {
          @Override
          public Object[] apply(final io.druid.data.input.Row row)
          {
            final Object[] retVal = new Object[fieldList.size()];
            for (RelDataTypeField field : fieldList) {
              retVal[field.getIndex()] = coerce(
                  row.getRaw(queryBuilder.getRowOrder().get(field.getIndex())),
                  field.getType().getSqlTypeName()
              );
            }
            return retVal;
          }
        }
    );
  }

  public static ColumnMetaData.Rep rep(final SqlTypeName sqlType)
  {
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.of(String.class);
    } else if (SqlTypeName.DATETIME_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.INTEGER) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.BIGINT) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.FLOAT || sqlType == SqlTypeName.DOUBLE) {
      return ColumnMetaData.Rep.of(Double.class);
    } else if (sqlType == SqlTypeName.OTHER) {
      return ColumnMetaData.Rep.of(Object.class);
    } else {
      throw new ISE("No rep for SQL type[%s]", sqlType);
    }
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
    } else if (sqlType == SqlTypeName.OTHER) {
      // Complex type got out somehow.
      coercedValue = value.getClass().getName();
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }

    return coercedValue;
  }
}
