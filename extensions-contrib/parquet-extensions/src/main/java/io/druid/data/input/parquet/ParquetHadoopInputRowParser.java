/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import java.util.List;

public class ParquetHadoopInputRowParser implements InputRowParser<GenericRecord>
{
  private final ParseSpec parseSpec;
  private final List<String> dimensions;

  @JsonCreator
  public ParquetHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;

    List<DimensionSchema> dimensionSchema = parseSpec.getDimensionsSpec().getDimensions();
    this.dimensions = Lists.newArrayList();
    for (DimensionSchema dim : dimensionSchema) {
      this.dimensions.add(dim.getName());
    }
  }

  /**
   * imitate avro extension {@link AvroStreamInputRowParser#parseGenericRecord(GenericRecord, ParseSpec, List, boolean)}
   */
  @Override
  public InputRow parse(GenericRecord record)
  {
    GenericRecordAsMap genericRecordAsMap = new GenericRecordAsMap(record, false);
    TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
    DateTime dateTime = timestampSpec.extractTimestamp(genericRecordAsMap);
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
    return new ParquetHadoopInputRowParser(parseSpec);
  }
}
