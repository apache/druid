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
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.impl.ParseSpec;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

@JsonTypeName("protobuf")
public class ProtoBufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger log = new Logger(ProtoBufInputRowParser.class);

  private final ParseSpec parseSpec;
  private Parser<String, Object> parser;
  private final String descriptorFileInClasspath;

  @JsonCreator
  public ProtoBufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("descriptor") String descriptorFileInClasspath
  )
  {
    this.parseSpec = parseSpec;
    this.descriptorFileInClasspath = descriptorFileInClasspath;
    this.parser = parseSpec.makeParser();
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ProtoBufInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ProtoBufInputRowParser(parseSpec, descriptorFileInClasspath);
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    final Descriptor descriptor = getDescriptor(descriptorFileInClasspath);
    DynamicMessage message;
    try {
      message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(input));
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(e, "Invalid protobuf exception");
    }
    String json = null;
    try {
      json = JsonFormat.printer().print(message);
    }
    catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    Map<String, Object> record = parser.parse(json);
    return new MapBasedInputRow(
        parseSpec.getTimestampSpec().extractTimestamp(record),
        parseSpec.getDimensionsSpec().getDimensionNames(),
        record
    );
  }

  private Descriptor getDescriptor(String descriptorFileInClassPath)
  {
    try {
      InputStream fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFileInClassPath);
      FileDescriptorSet set = FileDescriptorSet.parseFrom(fin);
      FileDescriptor file = FileDescriptor.buildFrom(
          set.getFile(0), new FileDescriptor[]
              {}
      );
      return file.getMessageTypes().get(0);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
