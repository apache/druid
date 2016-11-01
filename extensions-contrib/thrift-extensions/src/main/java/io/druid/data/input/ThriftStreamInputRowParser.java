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
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.thrift.TBaseAsMap;
import io.druid.data.input.thrift.util.ThriftUtils;
import io.druid.java.util.common.logger.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.List;

@JsonTypeName("thrift_stream")
public class ThriftStreamInputRowParser implements ByteBufferInputRowParser
{

  private static final Logger log = new Logger(ThriftStreamInputRowParser.class);

  private final ParseSpec parseSpec;
  private final List<String> dimensions;
  private final String tClassName;
  private final Class<? extends TBase> tClass;

  @JsonCreator
  public ThriftStreamInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("tClassName") String tClassName
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.tClassName = tClassName;
    try {
      this.tClass = Class.forName(tClassName).asSubclass(TBase.class);
    }
    catch (ClassNotFoundException e) {
      log.error(e, "Wrong config on TBase className: %s", tClassName);
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    byte[] data = input.array();

    TBase tBase;
    try {
      tBase = tClass.newInstance();
    }
    catch (InstantiationException | IllegalAccessException e) {
      log.error(e, "Unexpected Exception, maybe wrong config on TBase className: %s", tClassName);
      throw new RuntimeException(e);
    }

    try {
      ThriftUtils.detectAndDeserialize(data, tBase);
    }
    catch (TException e) {
      log.warn(e, "TException during deserialization with rawLog: %s", new String(data));
    }

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

  @JsonProperty
  public String gettClassName()
  {
    return tClassName;
  }

  @Override
  public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ThriftStreamInputRowParser(parseSpec, tClassName);
  }

}
