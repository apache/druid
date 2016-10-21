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
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.logger.Logger;

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
  private final MapInputRowParser mapParser;
  private final String descriptorFileInClasspath;

  @JsonCreator
  public ProtoBufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("descriptor") String descriptorFileInClasspath
  )
  {
    this.parseSpec = parseSpec;
    this.descriptorFileInClasspath = descriptorFileInClasspath;
      this.mapParser = new MapInputRowParser(this.parseSpec);
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
    // We should really create a ProtoBufBasedInputRow that does not need an intermediate map but accesses
    // the DynamicMessage directly...
    Map<String, Object> theMap = buildStringKeyMap(input);

    return mapParser.parse(theMap);
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    final Descriptor descriptor = getDescriptor(descriptorFileInClasspath);
    final Map<String, Object> theMap = Maps.newHashMap();

    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(input));
      Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();

      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : allFields.entrySet()) {
        String name = entry.getKey().getName();
        if (theMap.containsKey(name)) {
          continue;
          // Perhaps throw an exception here?
          // throw new RuntimeException("dupicate key " + name + " in " + message);
        }
        Object value = entry.getValue();
        if (value instanceof Descriptors.EnumValueDescriptor) {
          Descriptors.EnumValueDescriptor desc = (Descriptors.EnumValueDescriptor) value;
          value = desc.getName();
        }

        theMap.put(name, value);
      }

    }
    catch (InvalidProtocolBufferException e) {
      log.warn(e, "Problem with protobuf something");
    }
    return theMap;
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
