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

package org.apache.druid.spectator.histogram.sql;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.spectator.histogram.SpectatorHistogramAggregatorFactory;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Utility methods shared by SpectatorHistogram SQL aggregators.
 */
public class SpectatorHistogramSqlUtils
{
  private SpectatorHistogramSqlUtils()
  {
    // Utility class
  }

  /**
   * Finds an existing SpectatorHistogramAggregatorFactory that matches the given input expression.
   *
   * @param virtualColumnRegistry the virtual column registry
   * @param input the input DruidExpression
   * @param existingAggregations list of existing aggregations to search through
   * @return matching factory if found, null otherwise
   */
  @Nullable
  public static SpectatorHistogramAggregatorFactory findMatchingAggregatorFactory(
      final VirtualColumnRegistry virtualColumnRegistry,
      final DruidExpression input,
      final List<Aggregation> existingAggregations
  )
  {
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (factory instanceof SpectatorHistogramAggregatorFactory) {
          final SpectatorHistogramAggregatorFactory histogramFactory =
              (SpectatorHistogramAggregatorFactory) factory;
          if (inputMatches(virtualColumnRegistry, input, histogramFactory)) {
            return histogramFactory;
          }
        }
      }
    }
    return null;
  }

  /**
   * Checks if the input expression matches the given aggregator factory.
   */
  private static boolean inputMatches(
      final VirtualColumnRegistry virtualColumnRegistry,
      final DruidExpression input,
      final SpectatorHistogramAggregatorFactory factory
  )
  {
    final DruidExpression virtualInput =
        virtualColumnRegistry.findVirtualColumnExpressions(factory.requiredFields())
                             .stream()
                             .findFirst()
                             .orElse(null);

    if (virtualInput == null) {
      return input.isDirectColumnAccess() && input.getDirectColumn().equals(factory.getFieldName());
    } else {
      return virtualInput.equals(input);
    }
  }

  /**
   * Creates a new SpectatorHistogramAggregatorFactory for the given input expression.
   *
   * @param virtualColumnRegistry the virtual column registry
   * @param input the input DruidExpression
   * @param histogramName the name for the new aggregator factory
   * @return a new SpectatorHistogramAggregatorFactory
   */
  public static SpectatorHistogramAggregatorFactory createAggregatorFactory(
      final VirtualColumnRegistry virtualColumnRegistry,
      final DruidExpression input,
      final String histogramName
  )
  {
    if (input.isDirectColumnAccess()) {
      return new SpectatorHistogramAggregatorFactory(
          histogramName,
          input.getDirectColumn()
      );
    } else {
      String virtualColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          input,
          ColumnType.DOUBLE
      );
      return new SpectatorHistogramAggregatorFactory(histogramName, virtualColumnName);
    }
  }
}
