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

import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.segment.indexing.ReaderUtils;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidParquetReadSupport extends GroupReadSupport
{
  private static final Logger LOG = new Logger(DruidParquetReadSupport.class);

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
    JSONPathSpec flattenSpec = null;
    if (parseSpec instanceof ParquetParseSpec && ((ParquetParseSpec) parseSpec).getFlattenSpec() != null) {
      flattenSpec = ((ParquetParseSpec) parseSpec).getFlattenSpec();
    }
    Set<String> fullSchemaFields = fullSchema.getFields().stream().map(Type::getName).collect(Collectors.toSet());

    Set<String> requiredFields = ReaderUtils.getColumnsRequiredForIngestion(
        fullSchemaFields,
        parseSpec.getTimestampSpec(),
        parseSpec.getDimensionsSpec(),
        config.getSchema().getDataSchema().getTransformSpec(),
        config.getSchema().getDataSchema().getAggregators(),
        flattenSpec
    );

    for (Type type : fullSchema.getFields()) {
      if (requiredFields.contains(type.getName())) {
        partialFields.add(type);
      }
    }

    LOG.info("Parquet schema name[%s] with full schema[%s] requires fields[%s]", name, fullSchemaFields, requiredFields);

    return new MessageType(name, partialFields);
  }

  @Override
  public ReadContext init(InitContext context)
  {
    MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), getPartialReadSchema(context));
    return new ReadContext(requestedProjection);
  }
}
