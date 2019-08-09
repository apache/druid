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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.Result;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class QueryMaker
{
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final PlannerContext plannerContext;
  private final ObjectMapper jsonMapper;

  public QueryMaker(
      final QueryLifecycleFactory queryLifecycleFactory,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.plannerContext = plannerContext;
    this.jsonMapper = jsonMapper;
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  public Sequence<Object[]> runQuery(final DruidQuery druidQuery)
  {
    final Query query = druidQuery.getQuery();

    final Query innerMostQuery = findInnerMostQuery(query);
    if (plannerContext.getPlannerConfig().isRequireTimeCondition() &&
        innerMostQuery.getIntervals().equals(Intervals.ONLY_ETERNITY)) {
      throw new CannotBuildQueryException(
          "requireTimeCondition is enabled, all queries must include a filter condition on the __time column"
      );
    }

    if (query instanceof TimeseriesQuery) {
      return executeTimeseries(druidQuery, (TimeseriesQuery) query);
    } else if (query instanceof TopNQuery) {
      return executeTopN(druidQuery, (TopNQuery) query);
    } else if (query instanceof GroupByQuery) {
      return executeGroupBy(druidQuery, (GroupByQuery) query);
    } else if (query instanceof ScanQuery) {
      return executeScan(druidQuery, (ScanQuery) query);
    } else {
      throw new ISE("Cannot run query of class[%s]", query.getClass().getName());
    }
  }

  private Query findInnerMostQuery(Query outerQuery)
  {
    Query query = outerQuery;
    while (query.getDataSource() instanceof QueryDataSource) {
      query = ((QueryDataSource) query.getDataSource()).getQuery();
    }
    return query;
  }

  private Sequence<Object[]> executeScan(
      final DruidQuery druidQuery,
      final ScanQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();
    final RowSignature outputRowSignature = druidQuery.getOutputRowSignature();

    // SQL row column index -> Scan query column index
    final int[] columnMapping = new int[outputRowSignature.getRowOrder().size()];
    final Map<String, Integer> scanColumnOrder = new HashMap<>();

    for (int i = 0; i < query.getColumns().size(); i++) {
      scanColumnOrder.put(query.getColumns().get(i), i);
    }

    for (int i = 0; i < outputRowSignature.getRowOrder().size(); i++) {
      final Integer index = scanColumnOrder.get(outputRowSignature.getRowOrder().get(i));
      columnMapping[i] = index == null ? -1 : index;
    }

    return Sequences.concat(
        Sequences.map(
            runQuery(query),
            scanResult -> {
              final List<Object[]> retVals = new ArrayList<>();
              final List<List<Object>> rows = (List<List<Object>>) scanResult.getEvents();

              for (List<Object> row : rows) {
                final Object[] retVal = new Object[fieldList.size()];
                for (RelDataTypeField field : fieldList) {
                  retVal[field.getIndex()] = coerce(
                      row.get(columnMapping[field.getIndex()]),
                      field.getType().getSqlTypeName()
                  );
                }
                retVals.add(retVal);
              }

              return Sequences.simple(retVals);
            }
        )
    );
  }

  private <T> Sequence<T> runQuery(Query<T> query)
  {
    Hook.QUERY_PLAN.run(query);

    final String queryId = UUID.randomUUID().toString();
    plannerContext.addNativeQueryId(queryId);
    query = query.withId(queryId)
                 .withSqlQueryId(plannerContext.getSqlQueryId());

    final AuthenticationResult authenticationResult = plannerContext.getAuthenticationResult();
    return queryLifecycleFactory.factorize().runSimple(query, authenticationResult, null);
  }

  private Sequence<Object[]> executeTimeseries(
      final DruidQuery druidQuery,
      final TimeseriesQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();
    final String timeOutputName = druidQuery.getGrouping().getDimensions().isEmpty()
                                  ? null
                                  : Iterables.getOnlyElement(druidQuery.getGrouping().getDimensions())
                                             .getOutputName();

    return Sequences.map(
        runQuery(query),
        new Function<Result<TimeseriesResultValue>, Object[]>()
        {
          @Override
          public Object[] apply(final Result<TimeseriesResultValue> result)
          {
            final Map<String, Object> row = result.getValue().getBaseObject();
            final Object[] retVal = new Object[fieldList.size()];

            for (final RelDataTypeField field : fieldList) {
              final String outputName = druidQuery.getOutputRowSignature().getRowOrder().get(field.getIndex());
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
      final DruidQuery druidQuery,
      final TopNQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    return Sequences.concat(
        Sequences.map(
            runQuery(query),
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
                    final String outputName = druidQuery.getOutputRowSignature().getRowOrder().get(field.getIndex());
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
      final DruidQuery druidQuery,
      final GroupByQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();
    final Object2IntMap<String> resultRowPositionLookup = query.getResultRowPositionLookup();
    final List<String> sqlRowOrder = druidQuery.getOutputRowSignature().getRowOrder();
    final int[] resultRowPositions = new int[fieldList.size()];

    for (final RelDataTypeField field : fieldList) {
      final String columnName = sqlRowOrder.get(field.getIndex());
      final int resultRowPosition = resultRowPositionLookup.applyAsInt(columnName);
      resultRowPositions[field.getIndex()] = resultRowPosition;
    }

    return Sequences.map(
        runQuery(query),
        resultRow -> {
          final Object[] retVal = new Object[fieldList.size()];
          for (RelDataTypeField field : fieldList) {
            retVal[field.getIndex()] = coerce(
                resultRow.get(resultRowPositions[field.getIndex()]),
                field.getType().getSqlTypeName()
            );
          }
          return retVal;
        }
    );
  }

  public static ColumnMetaData.Rep rep(final SqlTypeName sqlType)
  {
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.of(String.class);
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.DATE) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.INTEGER) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.BIGINT) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.FLOAT) {
      return ColumnMetaData.Rep.of(Float.class);
    } else if (sqlType == SqlTypeName.DOUBLE || sqlType == SqlTypeName.DECIMAL) {
      return ColumnMetaData.Rep.of(Double.class);
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      return ColumnMetaData.Rep.of(Boolean.class);
    } else if (sqlType == SqlTypeName.OTHER) {
      return ColumnMetaData.Rep.of(Object.class);
    } else {
      throw new ISE("No rep for SQL type[%s]", sqlType);
    }
  }

  private Object coerce(final Object value, final SqlTypeName sqlType)
  {
    final Object coercedValue;

    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      if (value == null || value instanceof String) {
        coercedValue = NullHandling.nullToEmptyIfNeeded((String) value);
      } else if (value instanceof NlsString) {
        coercedValue = ((NlsString) value).getValue();
      } else if (value instanceof Number) {
        coercedValue = String.valueOf(value);
      } else if (value instanceof Collection) {
        // Iterate through the collection, coercing each value. Useful for handling selects of multi-value dimensions.
        final List<String> valueStrings = ((Collection<?>) value).stream()
                                                                 .map(v -> (String) coerce(v, sqlType))
                                                                 .collect(Collectors.toList());

        try {
          coercedValue = jsonMapper.writeValueAsString(valueStrings);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (value == null) {
      coercedValue = null;
    } else if (sqlType == SqlTypeName.DATE) {
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      if (value instanceof String) {
        coercedValue = Evals.asBoolean(((String) value));
      } else if (value instanceof Number) {
        coercedValue = Evals.asBoolean(((Number) value).longValue());
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
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToLong(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.FLOAT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToFloat(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlType)) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToDouble(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.OTHER) {
      // Complex type, try to serialize if we should, else print class name
      if (plannerContext.getPlannerConfig().shouldSerializeComplexValues()) {
        try {
          coercedValue = jsonMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException jex) {
          throw new ISE(jex, "Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
        }
      } else {
        coercedValue = value.getClass().getName();
      }
    } else if (sqlType == SqlTypeName.ARRAY) {
      try {
        coercedValue = jsonMapper.writeValueAsString(value);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }

    return coercedValue;
  }

  private static DateTime coerceDateTime(Object value, SqlTypeName sqlType)
  {
    final DateTime dateTime;

    if (value instanceof Number) {
      dateTime = DateTimes.utc(((Number) value).longValue());
    } else if (value instanceof String) {
      dateTime = DateTimes.utc(Long.parseLong((String) value));
    } else if (value instanceof DateTime) {
      dateTime = (DateTime) value;
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }
    return dateTime;
  }
}
