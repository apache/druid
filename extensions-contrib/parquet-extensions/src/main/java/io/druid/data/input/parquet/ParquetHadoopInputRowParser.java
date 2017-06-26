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

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

  @Nullable
  private LogicalType determineTimestampSpecLogicalType(Schema schema, String timestampSpecField)
  {
    for (Schema.Field field : schema.getFields()) {
      if (field.name().equals(timestampSpecField)) {
        return field.schema().getLogicalType();
      }
    }
    return null;
  }

  /**
   * imitate avro extension {@link AvroStreamInputRowParser#parseGenericRecord(GenericRecord, ParseSpec, List, boolean, boolean)}
   */
  @Override
  public InputRow parse(GenericRecord record)
  {
    // Map the record to a map
    GenericRecordAsMap genericRecordAsMap = new GenericRecordAsMap(record, false, binaryAsString);

    // Determine logical type of the timestamp column
    LogicalType logicalType = determineTimestampSpecLogicalType(record.getSchema(), timestampSpec.getTimestampColumn());

    // Parse time timestamp based on the parquet schema.
    // https://github.com/Parquet/parquet-format/blob/1afe8d9ae7e38acfc4ea273338a3c0c35feca115/LogicalTypes.md#date
    DateTime dateTime;
    if (logicalType instanceof LogicalTypes.Date) {
      int daysSinceEpoch = (Integer) genericRecordAsMap.get(timestampSpec.getTimestampColumn());

      dateTime = new DateTime(TimeUnit.DAYS.toMillis(daysSinceEpoch));
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
