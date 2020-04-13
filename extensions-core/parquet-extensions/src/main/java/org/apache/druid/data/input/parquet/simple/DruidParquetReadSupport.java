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

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DruidParquetReadSupport extends GroupReadSupport
{
  /**
   * Select the columns from the parquet schema that are used in the schema of the ingestion job
   *
   * @param context The context of the file to be read
   *
   * @return the partial schema that only contains the columns that are being used in the schema
   */
  private MessageType getPartialReadSchema(InitContext context)
  {
    MessageType fullSchema = context.getFileSchema();

    String name = fullSchema.getName();

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    ParseSpec parseSpec = config.getParser().getParseSpec();

    // this is kind of lame, maybe we can still trim what we read if we
    // parse the flatten spec and determine it isn't auto discovering props?
    if (parseSpec instanceof ParquetParseSpec) {
      if (((ParquetParseSpec) parseSpec).getFlattenSpec() != null) {
        return fullSchema;
      }
    }

    String tsField = parseSpec.getTimestampSpec().getTimestampColumn();

    List<DimensionSchema> dimensionSchema = parseSpec.getDimensionsSpec().getDimensions();
    Set<String> dimensions = new HashSet<>();
    for (DimensionSchema dim : dimensionSchema) {
      dimensions.add(dim.getName());
    }

    Set<String> metricsFields = new HashSet<>();
    for (AggregatorFactory agg : config.getSchema().getDataSchema().getAggregators()) {
      metricsFields.addAll(agg.requiredFields());
    }

    List<Type> partialFields = new ArrayList<>();

    for (Type type : fullSchema.getFields()) {
      if (tsField.equals(type.getName())
          || metricsFields.contains(type.getName())
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
