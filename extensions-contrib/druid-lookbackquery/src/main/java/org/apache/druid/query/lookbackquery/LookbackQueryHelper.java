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

package org.apache.druid.query.lookbackquery;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for LookbackQuery
 */
public class LookbackQueryHelper
{

  /**
   * Validates that the given DataSource is a QueryDataSource
   *
   * @param datasource DataSource to be validated
   */
  public static void validateDatasource(DataSource datasource)
  {
    Preconditions.checkArgument(
        (datasource instanceof QueryDataSource),
        "The datasource for a lookback query should be a QueryDataSource"
    );

  }

  /**
   * Validates that the given querySegmentSpec is an instance of MultipleIntervalSegmentSpec
   *
   * @param lookbackQuery The LookbackQuery to be validated
   */
  public static void validateQuerySegmentSpec(LookbackQuery lookbackQuery)
  {
    Query<?> innerQuery = ((QueryDataSource) lookbackQuery.getDataSource()).getQuery();
    QuerySegmentSpec querySegmentSpec = ((BaseQuery) innerQuery).getQuerySegmentSpec();

    Preconditions.checkArgument(
        (querySegmentSpec instanceof MultipleIntervalSegmentSpec),
        "The querySegmentSpec for a lookback query should be a MultipleIntervalSegmentSpec"
    );
  }

  /**
   * Validates that the lookbackOffsets and lookbackPrefixes are valid.
   *
   * @param lookbackOffsets  A list of Periods representing the lookback offsets
   * @param lookbackPrefixes A list of prefix values
   */
  public static void validateLookbackOffsetsAndPeriods(List<Period> lookbackOffsets, List<String> lookbackPrefixes)
  {
    Preconditions.checkNotNull(lookbackOffsets, "the lookbackOffsets cannot be null for a LookbackQuery");
    Preconditions.checkNotNull(lookbackPrefixes, "the lookbackPrefix list cannot be null");
    Preconditions.checkArgument(lookbackOffsets.size() > 0, "the lookbackOffsets must contain at least one period");
    Preconditions.checkArgument(
        lookbackOffsets.size() == lookbackPrefixes.size(),
        "must have equal number of lookback offsets and prefixes"
    );
    for (Period period : lookbackOffsets) {
      Preconditions.checkNotNull(period, "lookbackOffsets members must not be null");
    }
    for (String prefix : lookbackPrefixes) {
      Preconditions.checkNotNull(prefix, "lookback prefix entry must not be null");
    }
  }

  /**
   * Validates that the names of PostAggregators in the LookbackQuery do not clash with the names of Aggregators and
   * Postaggregators from the inner query
   *
   * @param lookbackQuery The LookbackQuery to be validated
   */
  public static void validatePostaggNameUniqueness(LookbackQuery lookbackQuery)
  {
    List<PostAggregator> lookBackQueryPostAggs = lookbackQuery.getPostAggregatorSpecs();
    List<DimensionSpec> dimensionSpecs = lookbackQuery.getDimensions();
    List<AggregatorFactory> innerAggs;
    List<PostAggregator> innerPostAggs;
    Set<String> fieldNames = new HashSet<>();

    if (lookBackQueryPostAggs.isEmpty()) {
      return;
    }

    Query<?> innerQuery = ((QueryDataSource) lookbackQuery.getDataSource()).getQuery();
    switch (innerQuery.getType()) {
      case Query.TIMESERIES:
        innerAggs = ((TimeseriesQuery) innerQuery).getAggregatorSpecs();
        innerPostAggs = ((TimeseriesQuery) innerQuery).getPostAggregatorSpecs();
        break;
      case Query.GROUP_BY:
        innerAggs = ((GroupByQuery) innerQuery).getAggregatorSpecs();
        innerPostAggs = ((GroupByQuery) innerQuery).getPostAggregatorSpecs();
        break;
      default:
        throw new ISE("Query type [%s]is not supported", innerQuery.getType());
    }

    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      fieldNames.add(dimensionSpec.getOutputName());
    }

    for (AggregatorFactory aggFactory : innerAggs) {
      fieldNames.add(aggFactory.getName());
    }

    for (PostAggregator postAggregator : innerPostAggs) {
      fieldNames.add(postAggregator.getName());
    }

    for (PostAggregator postAggregator : lookBackQueryPostAggs) {
      Preconditions.checkArgument(
          fieldNames.add(postAggregator.getName()),
          "[%s] already defined",
          postAggregator.getName()
      );
    }

  }

  /**
   * Build the default list of lookback prefixes from a given list of offsets. The default prefixes are
   * in the format "lookback_&lt;Period&gt;_", so for the period P-1D, the prefix would be "lookback_P-1D_".
   *
   * @param offsets List of offsets
   *
   * @return A list of prefixes, or null if offsets was null.
   */
  public static List<String> buildLookbackPrefixList(List<Period> offsets)
  {
    if (offsets == null) {
      return null;
    }
    return Lists.transform(offsets, new Function<Period, String>()
    {
      @Override
      public String apply(Period input)
      {
        return LookbackQuery.DEFAULT_LOOKBACK_PREFIX + input.toString() + "_";
      }
    });
  }

  /**
   * Returns a list of CohortInterval by adding the lookbackOffsets to each interval in the list provided
   *
   * @param currentInterval List of Intervals to be used for generating the cohort interval
   * @param lookbackOffset  Period to use as an offset
   *
   * @return A list of cohort Intervals
   */
  public static List<Interval> getCohortInterval(List<Interval> currentInterval, Period lookbackOffset)
  {

    List<Interval> cohortInterval = new ArrayList<>();

    for (Interval interval : currentInterval) {

      Interval lookbackInterval = new Interval(
          interval.getStart().withPeriodAdded(lookbackOffset, 1),
          interval.getEnd().withPeriodAdded(lookbackOffset, 1)
      );

      cohortInterval.add(lookbackInterval);
    }
    return cohortInterval;
  }

  /**
   * This query returns a map which can be used as the key to look up lookback values in case of a GroupBy query.
   * Map has one row of timestamp as key and timestamp value as value followed by all the dimensions from the provided
   * list and their corresponding values obtained by from the given row
   * <p>
   * Ex:
   * <pre>
   * [ timestamp - 2016-01-12T00:00:00.000Z
   * gender - m
   * browser - mozilla ]
   * </pre>
   *
   * @param dimensions A list of DimensionSpec to be used for generating the cohort key for a group by query
   * @param row        The Row to be used for looking up dimension values
   * @param datetime   The DateTime to be used as a value of timestamp
   *
   * @return A Map which can act as the key for a GroupBy query
   */
  public static Map<String, Object> getGroupByCohortMapKey(
      List<DimensionSpec> dimensions,
      Map<String, Object> row,
      DateTime datetime
  )
  {
    Map<String, Object> key = new HashMap<>();
    key.put("timestamp", datetime);

    for (DimensionSpec dimension : dimensions) {
      key.put(dimension.getOutputName(), row.get(dimension.getOutputName()));
    }
    return key;
  }
}
