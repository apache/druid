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

package /*CHECKSTYLE.OFF: PackageName*/org.apache.parquet.avro/*CHECKSTYLE.ON: PackageName*/;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.avro.AvroParseSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class must be in the package org.apache.parquet.avro to access {@link AvroRecordMaterializer}'s constructor
 */
public class DruidParquetAvroReadSupport extends AvroReadSupport<GenericRecord>
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

    if (parseSpec instanceof AvroParseSpec) {
      if (((AvroParseSpec) parseSpec).getFlattenSpec() != null) {
        return fullSchema;
      }
    }

    String tsField = config.getParser().getParseSpec().getTimestampSpec().getTimestampColumn();

    List<DimensionSchema> dimensionSchema = config.getParser().getParseSpec().getDimensionsSpec().getDimensions();
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

  @Override
  public RecordMaterializer<GenericRecord> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext
  )
  {
    // coercing this value to false by default here to be friendlier default behavior
    // see https://github.com/apache/incubator-druid/issues/5433#issuecomment-388539306
    String jobProp = "parquet.avro.add-list-element-records";
    Boolean explicitlySet = configuration.getBoolean(jobProp, false);
    if (!explicitlySet) {
      configuration.setBoolean(jobProp, false);
    }
    MessageType parquetSchema = readContext.getRequestedSchema();
    Schema avroSchema = new AvroSchemaConverter(configuration).convert(parquetSchema);

    Class<? extends AvroDataSupplier> suppClass = configuration.getClass(
        AVRO_DATA_SUPPLIER,
        SpecificDataSupplier.class,
        AvroDataSupplier.class
    );
    AvroDataSupplier supplier = ReflectionUtils.newInstance(suppClass, configuration);
    return new AvroRecordMaterializer<>(parquetSchema, avroSchema, supplier.get());
  }
}
