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

package io.druid.data.input.protobuf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.common.parsers.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;

public class ProtoBufInputRowParser implements ByteBufferInputRowParser {
  private final ParseSpec parseSpec;
  private Parser<String, Object> parser;
  private final String descriptorFilePath;
  private final String protoMessageType;
  private Descriptor descriptor;


  @JsonCreator
  public ProtoBufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("descriptor") String descriptorFilePath,
      @JsonProperty("protoMessageType") String protoMessageType
  ) {
    this.parseSpec = parseSpec;
    this.descriptorFilePath = descriptorFilePath;
    this.protoMessageType = protoMessageType;
    this.parser = parseSpec.makeParser();
    this.descriptor = getDescriptor(descriptorFilePath);
  }

  @Override
  public ParseSpec getParseSpec() {
    return parseSpec;
  }

  @Override
  public ProtoBufInputRowParser withParseSpec(ParseSpec parseSpec) {
    return new ProtoBufInputRowParser(parseSpec, descriptorFilePath, protoMessageType);
  }

  @Override
  public InputRow parse(ByteBuffer input) {
    String json;
    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(input));
      json = JsonFormat.printer().print(message);
    } catch (InvalidProtocolBufferException e) {
      throw new ParseException(e, "Protobuf message could not be parsed");
    }

    Map<String, Object> record = parser.parse(json);
    return new MapBasedInputRow(
        parseSpec.getTimestampSpec().extractTimestamp(record),
        parseSpec.getDimensionsSpec().getDimensionNames(),
        record
    );
  }

  private Descriptor getDescriptor(String descriptorFilePath) {
    InputStream fin;

    fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFilePath);
    if (fin == null) {
      URL url = null;
      try {
        url = new URL(descriptorFilePath);
      } catch (MalformedURLException e) {
        throw new ParseException(e, "descriptor has to be in the classpath or URL:" + descriptorFilePath);
      }
      try {
        fin = url.openConnection().getInputStream();
      } catch (IOException e) {
        throw new ParseException(e, "Cannot open descriptor file: " + url.toString());
      }
    }

    DynamicSchema dynamicSchema = null;
    try {
      dynamicSchema = DynamicSchema.parseFrom(fin);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new ParseException(e, "Invalid descriptor file: " + descriptorFilePath);
    } catch (IOException e) {
      throw new ParseException(e, "Cannot read " + descriptorFilePath);
    }

    Descriptor desc = dynamicSchema.getMessageDescriptor(protoMessageType);
    if (desc == null) {
      throw new ParseException(new NullPointerException(),
                               String.format(
                                   "ProtoBuf message type %s not found in the specified descriptor.  Available messages types are %s",
                                   protoMessageType,
                                   dynamicSchema.getMessageTypes()
                               )
      );
    }
    return desc;
  }
}
