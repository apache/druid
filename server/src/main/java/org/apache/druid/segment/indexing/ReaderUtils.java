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

package org.apache.druid.segment.indexing;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.transform.Transform;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReaderUtils
{
  private static final Logger LOG = new Logger(ReaderUtils.class);

  private static final Pattern JSON_PATH_PATTERN = Pattern.compile("\\[(.*?)]");
  private static final Pattern BRACKET_NOTATED_CHILD_PATTERN = Pattern.compile("'(.*?)'");

  public static Set<String> getColumnsRequiredForIngestion(
      Set<String> fullInputSchema,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      TransformSpec transformSpec,
      AggregatorFactory[] aggregators,
      @Nullable JSONPathSpec flattenSpec
  )
  {
    Set<String> fieldsRequired = new HashSet<>();

    // We need to read timestamp column for Druid timestamp field
    fieldsRequired.add(timestampSpec.getTimestampColumn());

    // Find columns we need to read from the flattenSpec
    if (flattenSpec != null) {
      if (dimensionsSpec.getDimensions().isEmpty() && flattenSpec.isUseFieldDiscovery()) {
        // Schemaless ingestion with useFieldDiscovery needs to read all columns
        return fullInputSchema;
      }

      // Parse columns needed from flattenSpec
      for (JSONPathFieldSpec fields : flattenSpec.getFields()) {
        if (fields.getType() == JSONPathFieldType.ROOT) {
          // ROOT type just get top level field using the expr as the key
          fieldsRequired.add(fields.getExpr());
        } else if (fields.getType() == JSONPathFieldType.PATH) {
          // Parse PATH type to determine columns needed
          String parsedPath;
          try {
            parsedPath = JSONPathFieldSpec.getCompilePath(fields.getExpr());
          }
          catch (Exception e) {
            // We can skip columns used in this path as the path is invalid
            LOG.debug("Ignoring columns from JSON path [%s] as path expression is invalid", fields.getExpr());
            continue;
          }
          // Remove the $
          parsedPath = parsedPath.substring(1);
          // If the first level is a deep scan, then we need all columns
          if (parsedPath.length() >= 2 && "..".equals(parsedPath.substring(0, 2))) {
            return fullInputSchema;
          }
          Matcher jsonPathMatcher = JSON_PATH_PATTERN.matcher(parsedPath);
          if (!jsonPathMatcher.find()) {
            LOG.warn("Failed to parse JSON path for required column from path [%s]", fields.getExpr());
            return fullInputSchema;
          }
          String matchedGroup = jsonPathMatcher.group();
          Matcher childMatcher = BRACKET_NOTATED_CHILD_PATTERN.matcher(matchedGroup);
          if (childMatcher.find()) {
            // Get name of the column from bracket-notated child i.e. ['region']
            childMatcher.reset();
            while (childMatcher.find()) {
              String columnName = childMatcher.group();
              // Remove the quote around column name
              fieldsRequired.add(columnName.substring(1, columnName.length() - 1));
            }
          } else if ("[*]".equals(matchedGroup)) {
            // If the first level is a wildcard, then we need all columns
            return fullInputSchema;
          } else {
            // This can happen if it is a filter expression, slice operator, or index / indexes
            // We just return all columns...
            return fullInputSchema;
          }
        } else {
          // Others type aren't supported but returning full schema just in case...
          LOG.warn("Got unexpected JSONPathFieldType [%s]", fields.getType());
          return fullInputSchema;
        }
      }
      // If useFieldDiscovery is false then we have already determined all the columns we need to read from
      // (as only explicitly specified fields will be available to use in the other specs)
      if (!flattenSpec.isUseFieldDiscovery()) {
        fieldsRequired.retainAll(fullInputSchema);
        return fieldsRequired;
      }
    } else {
      // Without flattenSpec, useFieldDiscovery is default to true and thus needs to read all columns since this is
      // schemaless
      if (dimensionsSpec.getDimensions().isEmpty()) {
        return fullInputSchema;
      }
    }

    // Determine any fields we need to read from input file that is used in the transformSpec
    List<Transform> transforms = transformSpec.getTransforms();
    for (Transform transform : transforms) {
      fieldsRequired.addAll(transform.getRequiredColumns());
    }

    // Determine any fields we need to read from input file that is used in the dimensionsSpec
    List<DimensionSchema> dimensionSchema = dimensionsSpec.getDimensions();
    for (DimensionSchema dim : dimensionSchema) {
      fieldsRequired.add(dim.getName());
    }

    // Determine any fields we need to read from input file that is used in the metricsSpec
    for (AggregatorFactory agg : aggregators) {
      fieldsRequired.addAll(agg.requiredFields());
    }

    // Only required fields that actually exist in the input schema
    fieldsRequired.retainAll(fullInputSchema);
    return fieldsRequired;
  }
}
