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

package org.apache.druid.indexing.input;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.transform.Transform;
import org.apache.druid.segment.transform.TransformSpec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities that are helpful when implementing {@link org.apache.druid.data.input.InputEntityReader}.
 */
public class InputRowSchemas
{
  private InputRowSchemas()
  {
    // No instantiation.
  }

  /**
   * Creates an {@link InputRowSchema} from a given {@link DataSchema}.
   */
  public static InputRowSchema fromDataSchema(final DataSchema dataSchema)
  {
    return new InputRowSchema(
        dataSchema.getTimestampSpec(),
        dataSchema.getDimensionsSpec(),
        createColumnsFilter(
            dataSchema.getTimestampSpec(),
            dataSchema.getDimensionsSpec(),
            dataSchema.getTransformSpec(),
            dataSchema.getAggregators()
        ),
        Arrays.stream(dataSchema.getAggregators())
              .map(AggregatorFactory::getName)
              .collect(Collectors.toSet())
    );
  }

  /**
   * Build a {@link ColumnsFilter} that can filter down the list of columns that must be read after flattening.
   *
   * @see InputRowSchema#getColumnsFilter()
   */
  @VisibleForTesting
  static ColumnsFilter createColumnsFilter(
      final TimestampSpec timestampSpec,
      final DimensionsSpec dimensionsSpec,
      final TransformSpec transformSpec,
      final AggregatorFactory[] aggregators
  )
  {
    // We'll need to know what fields are generated from transforms, vs. expected from the raw data.
    final Set<String> transformOutputNames =
        transformSpec.getTransforms().stream().map(Transform::getName).collect(Collectors.toSet());

    if (dimensionsSpec.hasCustomDimensions()) {
      // We need an inclusion-based filter.
      final Set<String> inclusions = new HashSet<>();

      // Add timestamp column.
      inclusions.add(timestampSpec.getTimestampColumn());

      // Add all transform inputs.
      inclusions.addAll(transformSpec.getRequiredColumns());

      // Add all dimension inputs that are *not* transform outputs.
      for (String column : dimensionsSpec.getDimensionNames()) {
        if (!transformOutputNames.contains(column)) {
          inclusions.add(column);
        }
      }

      // Add all aggregator inputs that are *not* transform outputs.
      for (AggregatorFactory aggregator : aggregators) {
        for (String column : aggregator.requiredFields()) {
          if (!transformOutputNames.contains(column)) {
            inclusions.add(column);
          }
        }
      }

      return ColumnsFilter.inclusionBased(inclusions);
    } else {
      // Schemaless dimensions mode: we need an exclusion-based filter.
      // Start from the list of dimension exclusions.
      final Set<String> exclusions = new HashSet<>(dimensionsSpec.getDimensionExclusions());

      // Remove (un-exclude) timestamp column.
      exclusions.remove(timestampSpec.getTimestampColumn());

      // Remove (un-exclude) all transform inputs.
      exclusions.removeAll(transformSpec.getRequiredColumns());

      // Remove (un-exclude) all aggregator inputs that are *not* transform outputs.
      for (AggregatorFactory aggregator : aggregators) {
        for (String column : aggregator.requiredFields()) {
          if (!transformOutputNames.contains(column)) {
            exclusions.remove(column);
          }
        }
      }

      return ColumnsFilter.exclusionBased(exclusions);
    }
  }
}
