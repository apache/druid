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

package org.apache.druid.data.input.parquet.simple;

import com.jayway.jsonpath.JsonPath;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.transform.Transform;
import org.apache.parquet.Log;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DruidParquetReadSupport extends GroupReadSupport
{
  private static final Logger LOG = new Logger(DruidParquetReadSupport.class);
  private static final Pattern JSON_PATH_PATTERN = Pattern.compile("\\[(.*?)\\]");


  /**
   * Select the columns from the parquet schema that are used in the schema of the ingestion job
   *
   * @param context The context of the file to be read
   *
   * @return the partial schema that only contains the columns that are being used in the schema
   */
  private MessageType getPartialReadSchema(InitContext context)
  {
    List<Type> partialFields = new ArrayList<>();

    MessageType fullSchema = context.getFileSchema();
    String name = fullSchema.getName();

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    ParseSpec parseSpec = config.getParser().getParseSpec();

    // We need to read timestamp column for Druid timestamp field
    String tsField = parseSpec.getTimestampSpec().getTimestampColumn();

    // Find columns of parquet file we need to read from the flattenSpec
    Set<String> columnsInFlattenSpec = new HashSet<>();
    if (parseSpec instanceof ParquetParseSpec && ((ParquetParseSpec) parseSpec).getFlattenSpec() != null) {
      // Parse columns needed from flattenSpec
      for (JSONPathFieldSpec fields : ((ParquetParseSpec) parseSpec).getFlattenSpec().getFields()) {
        if (fields.getType() == JSONPathFieldType.ROOT) {
          // ROOT type just get top level field using the expr as the key
          columnsInFlattenSpec.add(fields.getExpr());
        } else if (fields.getType() == JSONPathFieldType.PATH) {
          // Parse PATH type to determine columns needed
          String parsedPath;
          try {
            parsedPath = JsonPath.compile(fields.getExpr()).getPath();
          } catch (Exception e) {
            // We can skip columns used in this path as the path is invalid
            LOG.debug("Ignoring columns from JSON path [%s] as path expression is invalid", fields.getExpr());
            continue;
          }
          // Remove the $
          parsedPath = parsedPath.substring(1);
          // If the first level is a deep scan, then we need all columns
          if (parsedPath.length() >= 2 && "..".equals(parsedPath.substring(0, 2))) {
            return fullSchema;
          }
          Matcher matcher = JSON_PATH_PATTERN.matcher(parsedPath);
          if (!matcher.find()) {
            LOG.warn("Failed to parse JSON path for required column from path [%s]", fields.getExpr());
            return fullSchema;
          }
          String matchedGroup = matcher.group();
          if ("*".equals(matchedGroup)) {
            // If the first level is a wildcard, then we need all columns
            return fullSchema;
          } else if (matchedGroup.length() > 2 && matchedGroup.charAt(0) == '\'' &&  matchedGroup.charAt(matchedGroup.length() - 1) == '\'') {
            // Get name of the column
            columnsInFlattenSpec.add(matchedGroup.substring(1, matchedGroup.length() - 1));
          } else {
            // This can happen if it is a filter expression, slice operator, or index / indexes
            // We just return all columns...
            return fullSchema;
          }
        } else {
          // Others type aren't supported but returning full schema just in case...
          LOG.warn("Got unexpected JSONPathFieldType [%s]", fields.getType());
          return fullSchema;
        }
      }
      // If useFieldDiscovery is false then we have already determined all the columns we need to read from
      // (as only explicitly specified fields will be available to use in the other specs)
      if (!((ParquetParseSpec) parseSpec).getFlattenSpec().isUseFieldDiscovery()) {
        for (Type type : fullSchema.getFields()) {
          if (tsField.equals(type.getName())
              || columnsInFlattenSpec.size() > 0 && columnsInFlattenSpec.contains(type.getName())) {
            partialFields.add(type);
          }
        }
        return new MessageType(name, partialFields);
      }
    }

    // Determine any fields we need to read from parquet file that is used in the transformSpec
    Set<String> transformColumns = new HashSet<>();
    List<Transform> transforms = config.getSchema().getDataSchema().getTransformSpec().getTransforms();
    for (Transform transform : transforms) {
      transformColumns.addAll(transform.getRequiredColumns());
    }

    // Determine any fields we need to read from parquet file that is used in the dimensionsSpec
    List<DimensionSchema> dimensionSchema = parseSpec.getDimensionsSpec().getDimensions();
    Set<String> dimensions = new HashSet<>();
    for (DimensionSchema dim : dimensionSchema) {
      dimensions.add(dim.getName());
    }

    // Determine any fields we need to read from parquet file that is used in the metricsSpec
    Set<String> metricsFields = new HashSet<>();
    for (AggregatorFactory agg : config.getSchema().getDataSchema().getAggregators()) {
      metricsFields.addAll(agg.requiredFields());
    }


    for (Type type : fullSchema.getFields()) {
      if (tsField.equals(type.getName())
          || metricsFields.contains(type.getName())
          || columnsInFlattenSpec.size() > 0 && columnsInFlattenSpec.contains(type.getName())
          || transformColumns.size() > 0 && transformColumns.contains(type.getName())
          || dimensions.size() > 0 && dimensions.contains(type.getName())
          || dimensions.size() == 0) {
        partialFields.add(type);
      }
    }

    return new MessageType(name, partialFields);
  }

  @Override
  public ReadContext init(InitContext context)
  {
    MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), getPartialReadSchema(context));
    return new ReadContext(requestedProjection);
  }
}
