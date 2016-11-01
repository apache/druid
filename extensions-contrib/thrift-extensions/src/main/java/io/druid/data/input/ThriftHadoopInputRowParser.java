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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.thrift.TBaseAsMap;
import org.apache.thrift.TBase;
import org.joda.time.DateTime;

import java.util.List;

@JsonTypeName("thrift_hadoop")
public class ThriftHadoopInputRowParser implements InputRowParser<ThriftWritable<?>>
{

  private final ParseSpec parseSpec;
  private final List<String> dimensions;

  @JsonCreator
  public ThriftHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
  }

  @Override
  public InputRow parse(ThriftWritable<?> input)
  {
    TBase tBase = input.get();
    @SuppressWarnings("unchecked")
    TBaseAsMap<?> tBaseAsMap = new TBaseAsMap(tBase);
    TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
    DateTime dateTime = timestampSpec.extractTimestamp(tBaseAsMap);
    return new MapBasedInputRow(dateTime, dimensions, tBaseAsMap);
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
    return new ThriftHadoopInputRowParser(parseSpec);
  }

}
