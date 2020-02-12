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

package org.apache.druid.data.input.parquet.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.avro.AvroFlattenerMaker;
import org.apache.druid.data.input.avro.AvroParseSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ParquetAvroHadoopInputRowParser implements InputRowParser<GenericRecord>
{
  private final ParseSpec parseSpec;
  private final boolean binaryAsString;
  private final TimestampSpec timestampSpec;
  private final ObjectFlattener<GenericRecord> recordFlattener;
  private final List<String> dimensions;

  @JsonCreator
  public ParquetAvroHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("binaryAsString") Boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.timestampSpec = parseSpec.getTimestampSpec();
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;

    final JSONPathSpec flattenSpec;
    if (parseSpec instanceof AvroParseSpec) {
      flattenSpec = ((AvroParseSpec) parseSpec).getFlattenSpec();
    } else {
      flattenSpec = JSONPathSpec.DEFAULT;
    }

    this.recordFlattener = ObjectFlatteners.create(
        flattenSpec,
        new AvroFlattenerMaker(false, this.binaryAsString)
    );
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
   * imitate avro extension {@link org.apache.druid.data.input.avro.AvroParsers#parseGenericRecord}
   */
  @Nonnull
  @Override
  public List<InputRow> parseBatch(GenericRecord record)
  {
    Map<String, Object> row = recordFlattener.flatten(record);

    final List<String> dimensions;
    if (!this.dimensions.isEmpty()) {
      dimensions = this.dimensions;
    } else {
      dimensions = Lists.newArrayList(
          Sets.difference(row.keySet(), parseSpec.getDimensionsSpec().getDimensionExclusions())
      );
    }
    // check for parquet Date
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date
    LogicalType logicalType = determineTimestampSpecLogicalType(record.getSchema(), timestampSpec.getTimestampColumn());
    DateTime dateTime;
    if (logicalType instanceof LogicalTypes.Date) {
      int daysSinceEpoch = (Integer) record.get(timestampSpec.getTimestampColumn());
      dateTime = DateTimes.utc(TimeUnit.DAYS.toMillis(daysSinceEpoch));
    } else {
      // Fall back to a binary format that will be parsed using joda-time
      dateTime = timestampSpec.extractTimestamp(row);
    }
    return ImmutableList.of(new MapBasedInputRow(dateTime, dimensions, row));
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
    return new ParquetAvroHadoopInputRowParser(parseSpec, binaryAsString);
  }
}
