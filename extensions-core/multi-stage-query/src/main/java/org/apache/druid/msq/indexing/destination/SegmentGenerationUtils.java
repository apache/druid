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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.util.ArrayIngestMode;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.PassthroughAggregatorFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SegmentGenerationUtils
{
  private static final Logger log = new Logger(SegmentGenerationUtils.class);

  public static DataSchema makeDataSchemaForIngestion(
      MSQSpec querySpec,
      RowSignature querySignature,
      ClusterBy queryClusterBy,
      ColumnMappings columnMappings,
      ObjectMapper jsonMapper
  )
  {
    final DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
    final boolean isRollupQuery = isRollupQuery(querySpec.getQuery());
    final boolean forceSegmentSortByTime =
        MultiStageQueryContext.isForceSegmentSortByTime(querySpec.getQuery().context());

    final Pair<DimensionsSpec, List<AggregatorFactory>> dimensionsAndAggregators =
        makeDimensionsAndAggregatorsForIngestion(
            querySignature,
            queryClusterBy,
            destination.getSegmentSortOrder(),
            forceSegmentSortByTime,
            columnMappings,
            isRollupQuery,
            querySpec.getQuery(),
            destination.getDimensionSchemas()
        );

    return DataSchema.builder()
                     .withDataSource(destination.getDataSource())
                     .withTimestamp(new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null))
                     .withDimensions(dimensionsAndAggregators.lhs)
                     .withAggregators(dimensionsAndAggregators.rhs.toArray(new AggregatorFactory[0]))
                     .withGranularity(makeGranularitySpecForIngestion(querySpec.getQuery(), querySpec.getColumnMappings(), isRollupQuery, jsonMapper))
                     .build();
  }

  private static GranularitySpec makeGranularitySpecForIngestion(
      final Query<?> query,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final ObjectMapper jsonMapper
  )
  {
    if (isRollupQuery) {
      final String queryGranularityString =
          query.context().getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, "");

      if (timeIsGroupByDimension((GroupByQuery) query, columnMappings) && !queryGranularityString.isEmpty()) {
        final Granularity queryGranularity;

        try {
          queryGranularity = jsonMapper.readValue(queryGranularityString, Granularity.class);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }

        return new ArbitraryGranularitySpec(queryGranularity, true, Intervals.ONLY_ETERNITY);
      }
      return new ArbitraryGranularitySpec(Granularities.NONE, true, Intervals.ONLY_ETERNITY);
    } else {
      return new ArbitraryGranularitySpec(Granularities.NONE, false, Intervals.ONLY_ETERNITY);
    }
  }

  /**
   * Checks that a {@link GroupByQuery} is grouping on the primary time column.
   * <p>
   * The logic here is roundabout. First, we check which column in the {@link GroupByQuery} corresponds to the
   * output column {@link ColumnHolder#TIME_COLUMN_NAME}, using our {@link ColumnMappings}. Then, we check for the
   * presence of an optimization done in {@link DruidQuery#toGroupByQuery()}, where the context parameter
   * {@link GroupByQuery#CTX_TIMESTAMP_RESULT_FIELD} and various related parameters are set when one of the dimensions
   * is detected to be a time-floor. Finally, we check that the name of that dimension, and the name of our time field
   * from {@link ColumnMappings}, are the same.
   */
  private static boolean timeIsGroupByDimension(GroupByQuery groupByQuery, ColumnMappings columnMappings)
  {
    final IntList positions = columnMappings.getOutputColumnsByName(ColumnHolder.TIME_COLUMN_NAME);

    if (positions.size() == 1) {
      final String queryTimeColumn = columnMappings.getQueryColumnName(positions.getInt(0));
      return queryTimeColumn.equals(groupByQuery.context().getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD));
    } else {
      return false;
    }
  }

  /**
   * Whether a native query represents an ingestion with rollup.
   * <p>
   * Checks for three things:
   * <p>
   * - The query must be a {@link GroupByQuery}, because rollup requires columns to be split into dimensions and
   * aggregations.
   * - The query must not finalize aggregations, because rollup requires inserting the intermediate type of
   * complex aggregations, not the finalized type. (So further rollup is possible.)
   * - The query must explicitly disable {@link GroupByQueryConfig#CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING}, because
   * groupBy on multi-value dimensions implicitly unnests, which is not desired behavior for rollup at ingestion time
   * (rollup expects multi-value dimensions to be treated as arrays).
   */
  private static boolean isRollupQuery(Query<?> query)
  {
    return query instanceof GroupByQuery
           && !MultiStageQueryContext.isFinalizeAggregations(query.context())
           && !query.context().getBoolean(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, true);
  }

  private static DimensionSchema getDimensionSchema(
      final String outputColumnName,
      @Nullable final ColumnType queryType,
      QueryContext context,
      @Nullable Map<String, DimensionSchema> dimensionSchemas
  )
  {
    if (dimensionSchemas != null && dimensionSchemas.containsKey(outputColumnName)) {
      return dimensionSchemas.get(outputColumnName);
    }
    // In case of ingestion, or when metrics are converted to dimensions when compaction is performed without rollu

    // we won't have an entry in the map. For those cases, use the default config.
    return DimensionSchemaUtils.createDimensionSchema(
        outputColumnName,
        queryType,
        MultiStageQueryContext.useAutoColumnSchemas(context),
        MultiStageQueryContext.getArrayIngestMode(context)
    );
  }

  private static Pair<DimensionsSpec, List<AggregatorFactory>> makeDimensionsAndAggregatorsForIngestion(
      final RowSignature querySignature,
      final ClusterBy queryClusterBy,
      final List<String> contextSegmentSortOrder,
      final boolean forceSegmentSortByTime,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final Query<?> query,
      @Nullable final Map<String, DimensionSchema> dimensionSchemas
  )
  {
    // Log a warning unconditionally if arrayIngestMode is MVD, since the behaviour is incorrect, and is subject to
    // deprecation and removal in future
    if (MultiStageQueryContext.getArrayIngestMode(query.context()) == ArrayIngestMode.MVD) {
      log.warn(
          "%s[mvd] is active for this task. This causes string arrays (VARCHAR ARRAY in SQL) to be ingested as "
          + "multi-value strings rather than true arrays. This behavior may change in a future version of Druid. To be "
          + "compatible with future behavior changes, we recommend setting %s to[array], which creates a clearer "
          + "separation between multi-value strings and true arrays. In either[mvd] or[array] mode, you can write "
          + "out multi-value string dimensions using ARRAY_TO_MV. "
          + "See https://druid.apache.org/docs/latest/querying/arrays#arrayingestmode for more details.",
          MultiStageQueryContext.CTX_ARRAY_INGEST_MODE,
          MultiStageQueryContext.CTX_ARRAY_INGEST_MODE
      );
    }

    final List<DimensionSchema> dimensions = new ArrayList<>();
    final List<AggregatorFactory> aggregators = new ArrayList<>();

    // During ingestion, segment sort order is determined by the order of fields in the DimensionsSchema. We want
    // this to match user intent as dictated by the declared segment sort order and CLUSTERED BY, so add things in
    // that order.

    // Start with segmentSortOrder.
    final Set<String> outputColumnsInOrder = new LinkedHashSet<>(contextSegmentSortOrder);

    // Then __time, if it's an output column and forceSegmentSortByTime is set.
    if (columnMappings.hasOutputColumn(ColumnHolder.TIME_COLUMN_NAME) && forceSegmentSortByTime) {
      outputColumnsInOrder.add(ColumnHolder.TIME_COLUMN_NAME);
    }

    // Then the query-level CLUSTERED BY.
    // Note: this doesn't work when CLUSTERED BY specifies an expression that is not being selected.
    // Such fields in CLUSTERED BY still control partitioning as expected, but do not affect sort order of rows
    // within an individual segment.
    for (final KeyColumn clusterByColumn : queryClusterBy.getColumns()) {
      final IntList outputColumns = columnMappings.getOutputColumnsForQueryColumn(clusterByColumn.columnName());
      for (final int outputColumn : outputColumns) {
        outputColumnsInOrder.add(columnMappings.getOutputColumnName(outputColumn));
      }
    }

    // Then all other columns.
    outputColumnsInOrder.addAll(columnMappings.getOutputColumnNames());

    Map<String, AggregatorFactory> outputColumnAggregatorFactories = new HashMap<>();

    if (isRollupQuery) {
      // Populate aggregators from the native query when doing an ingest in rollup mode.
      for (AggregatorFactory aggregatorFactory : ((GroupByQuery) query).getAggregatorSpecs()) {
        for (final int outputColumn : columnMappings.getOutputColumnsForQueryColumn(aggregatorFactory.getName())) {
          final String outputColumnName = columnMappings.getOutputColumnName(outputColumn);
          if (outputColumnAggregatorFactories.containsKey(outputColumnName)) {
            throw new ISE("There can only be one aggregation for column [%s].", outputColumn);
          } else {
            outputColumnAggregatorFactories.put(
                outputColumnName,
                aggregatorFactory.withName(outputColumnName).getCombiningFactory()
            );
          }
        }
      }
    }

    // Each column can be either a dimension or an aggregator.
    // For non-complex columns, If the aggregator factory of the column is not available, we treat the column as
    // a dimension. For complex columns, certains hacks are in place.
    for (final String outputColumnName : outputColumnsInOrder) {
      // CollectionUtils.getOnlyElement because this method is only called during ingestion, where we require
      // that output names be unique.
      final int outputColumn = CollectionUtils.getOnlyElement(
          columnMappings.getOutputColumnsByName(outputColumnName),
          xs -> new ISE("Expected single output column for name [%s], but got [%s]", outputColumnName, xs)
      );
      final String queryColumn = columnMappings.getQueryColumnName(outputColumn);
      final ColumnType type =
          querySignature.getColumnType(queryColumn)
                        .orElseThrow(() -> new ISE("No type for column [%s]", outputColumnName));

      if (!type.is(ValueType.COMPLEX)) {
        // non complex columns
        populateDimensionsAndAggregators(
            dimensions,
            aggregators,
            outputColumnAggregatorFactories,
            outputColumnName,
            type,
            query.context(),
            dimensionSchemas
        );
      } else {
        // complex columns only
        if (DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.containsKey(type.getComplexTypeName())) {
          dimensions.add(
              getDimensionSchema(outputColumnName, type, query.context(), dimensionSchemas)
          );
        } else if (!isRollupQuery) {
          aggregators.add(new PassthroughAggregatorFactory(outputColumnName, type.getComplexTypeName()));
        } else {
          populateDimensionsAndAggregators(
              dimensions,
              aggregators,
              outputColumnAggregatorFactories,
              outputColumnName,
              type,
              query.context(),
              dimensionSchemas
          );
        }
      }
    }

    final DimensionsSpec.Builder dimensionsSpecBuilder = DimensionsSpec.builder();

    if (!dimensions.isEmpty() && dimensions.get(0).getName().equals(ColumnHolder.TIME_COLUMN_NAME)) {
      // Skip __time if it's in the first position, for compatibility with legacy dimensionSpecs.
      dimensions.remove(0);
      dimensionsSpecBuilder.setForceSegmentSortByTime(null);
    } else {
      // Store explicit forceSegmentSortByTime only if false, for compatibility with legacy dimensionSpecs.
      dimensionsSpecBuilder.setForceSegmentSortByTime(forceSegmentSortByTime ? null : false);
    }

    return Pair.of(dimensionsSpecBuilder.setDimensions(dimensions).build(), aggregators);
  }

  /**
   * If the output column is present in the outputColumnAggregatorFactories that means we already have the aggregator information for this column.
   * else treat this column as a dimension.
   *
   * @param dimensions                      list is poulated if the output col is deemed to be a dimension
   * @param aggregators                     list is populated with the aggregator if the output col is deemed to be a aggregation column.
   * @param outputColumnAggregatorFactories output col -> AggregatorFactory map
   * @param outputColumn                    column name
   * @param type                            columnType
   */
  private static void populateDimensionsAndAggregators(
      List<DimensionSchema> dimensions,
      List<AggregatorFactory> aggregators,
      Map<String, AggregatorFactory> outputColumnAggregatorFactories,
      String outputColumn,
      ColumnType type,
      QueryContext context,
      Map<String, DimensionSchema> dimensionSchemas
  )
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(outputColumn)) {
      if (!type.is(ValueType.LONG)) {
        throw DruidException.defensive("Incorrect type[%s] for column[%s]", type, outputColumn);
      }
      dimensions.add(new LongDimensionSchema(outputColumn));
    } else if (outputColumnAggregatorFactories.containsKey(outputColumn)) {
      aggregators.add(outputColumnAggregatorFactories.get(outputColumn));
    } else {
      dimensions.add(
          getDimensionSchema(outputColumn, type, context, dimensionSchemas)
      );
    }
  }

  private SegmentGenerationUtils()
  {
  }
}
