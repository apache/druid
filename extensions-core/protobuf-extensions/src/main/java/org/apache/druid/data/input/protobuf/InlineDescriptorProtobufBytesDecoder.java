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
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

public class InlineDescriptorProtobufBytesDecoder implements ProtobufBytesDecoder
{
  private final String descriptorString;
  private final String protoMessageType;
  private Descriptors.Descriptor descriptor;


  @JsonCreator
  public InlineDescriptorProtobufBytesDecoder(
      @JsonProperty("descriptorString") String descriptorString,
      @JsonProperty("protoMessageType") String protoMessageType
  )
  {
    this.descriptorString = descriptorString;
    this.protoMessageType = protoMessageType;
    initDescriptor();
  }

  @JsonProperty
  public String getDescriptorString()
  {
    return descriptorString;
  }

  @JsonProperty
  public String getProtoMessageType()
  {
    return protoMessageType;
  }

  @VisibleForTesting
  void initDescriptor()
  {
    if (this.descriptor == null) {
      this.descriptor = getDescriptor(descriptorString);
    }
  }

  @Override
  public DynamicMessage parse(ByteBuffer bytes)
  {
    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(bytes));
      return message;
    }
    catch (Exception e) {
      throw new ParseException(null, e, "Fail to decode protobuf message!");
    }
  }

  private Descriptors.Descriptor getDescriptor(String descriptorString)
  {
    DynamicSchema dynamicSchema;
    try {
      byte[] decodedDesc = StringUtils.decodeBase64String(descriptorString);
      dynamicSchema = DynamicSchema.parseFrom(decodedDesc);
    }
    catch (Descriptors.DescriptorValidationException e) {
      throw new ParseException(null, e, "Invalid descriptor string: " + descriptorString);
    }
    catch (IOException e) {
      throw new ParseException(null, e, "Cannot read descriptor string: " + descriptorString);
    }

    Set<String> messageTypes = dynamicSchema.getMessageTypes();
    if (messageTypes.size() == 0) {
      throw new ParseException(null, "No message types found in the descriptor: " + descriptorString);
    }

    String messageType = protoMessageType == null ? (String) messageTypes.toArray()[0] : protoMessageType;
    Descriptors.Descriptor desc = dynamicSchema.getMessageDescriptor(messageType);
    if (desc == null) {
      throw new ParseException(
          null,
          StringUtils.format(
              "Protobuf message type %s not found in the specified descriptor.  Available messages types are %s",
              protoMessageType,
              messageTypes
          )
      );
    }
    return desc;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InlineDescriptorProtobufBytesDecoder that = (InlineDescriptorProtobufBytesDecoder) o;

    return Objects.equals(descriptorString, that.descriptorString) &&
           Objects.equals(protoMessageType, that.protoMessageType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(descriptorString, protoMessageType);
  }

}
