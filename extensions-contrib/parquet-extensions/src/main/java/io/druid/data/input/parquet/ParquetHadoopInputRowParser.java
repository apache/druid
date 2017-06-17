/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.data.input.AvroStreamInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.avro.GenericRecordAsMap;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import java.util.List;

public class ParquetHadoopInputRowParser implements InputRowParser<GenericRecord>
{
  private final ParseSpec parseSpec;
  private final boolean binaryAsString;
  private final List<String> dimensions;
  private final TimestampSpec timestampSpec;

  @JsonCreator
  public ParquetHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("binaryAsString") Boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.timestampSpec = parseSpec.getTimestampSpec();
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;

    List<DimensionSchema> dimensionSchema = parseSpec.getDimensionsSpec().getDimensions();
    this.dimensions = Lists.newArrayList();
    for (DimensionSchema dim : dimensionSchema) {
      this.dimensions.add(dim.getName());
    }
  }

  private static LogicalType _timestampSpecLogicalType = null;

  private LogicalType determineTimestampSpecLogicalType(Schema schema, String timestampSpecFields)
  {
    for (Schema.Field field : schema.getFields()) {
      if (field.name().equals(timestampSpecFields)) {
        return field.schema().getLogicalType();
      }
    }
    // The timestamp field does not exists in the provided schema
    return null;
  }

  /**
   * imitate avro extension {@link AvroStreamInputRowParser#parseGenericRecord(GenericRecord, ParseSpec, List, boolean, boolean)}
   */
  @Override
  public InputRow parse(GenericRecord record)
  {
    LogicalType logicalType;

    // We don't want to determine the type every time if we already done so
    if (null != _timestampSpecLogicalType) {
      logicalType = _timestampSpecLogicalType;
    } else {
      logicalType = determineTimestampSpecLogicalType(record.getSchema(), timestampSpec.getTimestampColumn());
    }

    GenericRecordAsMap genericRecordAsMap = new GenericRecordAsMap(record, false, binaryAsString);

    // Parse time timestamp based on the parquet schema.
    // https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md#date
    DateTime dateTime;
    if (logicalType instanceof LogicalTypes.Date) {
      int daysSinceEpoch = (Integer) genericRecordAsMap.get(timestampSpec.getTimestampColumn());

      // days since epoch
      long DAY_IN_MILISECONDS = 1000L * 60L * 60L * 24L;
      dateTime = new DateTime(daysSinceEpoch * DAY_IN_MILISECONDS);
    } else {
      // Fall back to a binary format that will be parsed using joda-time
      dateTime = timestampSpec.extractTimestamp(genericRecordAsMap);
    }

    return new MapBasedInputRow(dateTime, dimensions, genericRecordAsMap);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ParquetHadoopInputRowParser(parseSpec, binaryAsString);
  }
}
