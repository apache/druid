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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

public class FileBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{
  private final String descriptorFilePath;
  private final String protoMessageType;
  private Descriptors.Descriptor descriptor;


  @JsonCreator
  public FileBasedProtobufBytesDecoder(
      @JsonProperty("descriptor") String descriptorFilePath,
      @JsonProperty("protoMessageType") String protoMessageType
  )
  {
    this.descriptorFilePath = descriptorFilePath;
    this.protoMessageType = protoMessageType;
    initDescriptor();
  }

  @JsonProperty
  public String getDescriptor()
  {
    return descriptorFilePath;
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
      this.descriptor = getDescriptor(descriptorFilePath);
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
      throw new ParseException(e, "Fail to decode protobuf message!");
    }
  }

  private Descriptors.Descriptor getDescriptor(String descriptorFilePath)
  {
    InputStream fin;

    fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFilePath);
    if (fin == null) {
      URL url;
      try {
        url = new URL(descriptorFilePath);
      }
      catch (MalformedURLException e) {
        throw new ParseException(e, "Descriptor not found in class path or malformed URL:" + descriptorFilePath);
      }
      try {
        fin = url.openConnection().getInputStream();
      }
      catch (IOException e) {
        throw new ParseException(e, "Cannot read descriptor file: " + url);
      }
    }
    DynamicSchema dynamicSchema;
    try {
      dynamicSchema = DynamicSchema.parseFrom(fin);
    }
    catch (Descriptors.DescriptorValidationException e) {
      throw new ParseException(e, "Invalid descriptor file: " + descriptorFilePath);
    }
    catch (IOException e) {
      throw new ParseException(e, "Cannot read descriptor file: " + descriptorFilePath);
    }

    Set<String> messageTypes = dynamicSchema.getMessageTypes();
    if (messageTypes.size() == 0) {
      throw new ParseException("No message types found in the descriptor: " + descriptorFilePath);
    }

    String messageType = protoMessageType == null ? (String) messageTypes.toArray()[0] : protoMessageType;
    Descriptors.Descriptor desc = dynamicSchema.getMessageDescriptor(messageType);
    if (desc == null) {
      throw new ParseException(
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

    FileBasedProtobufBytesDecoder that = (FileBasedProtobufBytesDecoder) o;

    return Objects.equals(descriptorFilePath, that.descriptorFilePath) &&
        Objects.equals(protoMessageType, that.protoMessageType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(descriptorFilePath, protoMessageType);
  }

}
