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

package org.apache.druid.data.input.protobuf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.druid.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ProtobufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger LOG = new Logger(ByteBufferInputRowParser.class);

  private final ParseSpec parseSpec;
  private final ProtobufBytesDecoder protobufBytesDecoder;
  private Parser<String, Object> parser;
  private final List<String> dimensions;

  @JsonCreator
  public ProtobufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("protoBytesDecoder") ProtobufBytesDecoder protobufBytesDecoder,
      @Deprecated
      @JsonProperty("descriptor") String descriptorFilePath,
      @Deprecated
      @JsonProperty("protoMessageType") String protoMessageType
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();

    if (descriptorFilePath != null || protoMessageType != null) {
      this.protobufBytesDecoder = new FileBasedProtobufBytesDecoder(descriptorFilePath, protoMessageType);
    } else {
      this.protobufBytesDecoder = protobufBytesDecoder;
    }
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ProtobufInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ProtobufInputRowParser(parseSpec, protobufBytesDecoder, null, null);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    if (parser == null) {
      parser = parseSpec.makeParser();
    }
    Map<String, Object> record;

    if (parseSpec instanceof JSONParseSpec && ((JSONParseSpec) parseSpec).getFlattenSpec().getFields().isEmpty()) {
      try {
        DynamicMessage message = protobufBytesDecoder.parse(input);
        record = CollectionUtils.mapKeys(message.getAllFields(), k -> k.getJsonName());
      }
      catch (Exception ex) {
        throw new ParseException(ex, "Protobuf message could not be parsed");
      }
    } else {
      try {
        DynamicMessage message = protobufBytesDecoder.parse(input);
        String json = JsonFormat.printer().print(message);
        record = parser.parseToMap(json);
      }
      catch (InvalidProtocolBufferException e) {
        throw new ParseException(e, "Protobuf message could not be parsed");
      }
    }

    final List<String> dimensions;
    if (!this.dimensions.isEmpty()) {
      dimensions = this.dimensions;
    } else {
      dimensions = Lists.newArrayList(
          Sets.difference(record.keySet(), parseSpec.getDimensionsSpec().getDimensionExclusions())
      );
    }
    return ImmutableList.of(new MapBasedInputRow(
        parseSpec.getTimestampSpec().extractTimestamp(record),
        dimensions,
        record
    ));
  }
}
