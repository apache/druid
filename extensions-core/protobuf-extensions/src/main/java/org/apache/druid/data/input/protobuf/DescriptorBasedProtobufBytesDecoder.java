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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base class for protobuf bytes decoders that use Google's protobuf library directly
 * to parse binary descriptor sets and decode protobuf messages.
 *
 * <p>This class provides common functionality for loading protobuf descriptors from various sources
 * and decoding binary protobuf messages into {@link DynamicMessage} objects. Concrete implementations
 * define how to obtain the protobuf descriptor set (e.g., from files or inline base64 strings).
 */
public abstract class DescriptorBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{
  private Descriptors.Descriptor descriptor;

  /**
   * An optional message Protobuf message type in the descriptor. Both short name and fully qualified name are accepted.
   * If not specified, the first message type found in the descriptor will be used.
   */
  @Nullable
  private final String protoMessageType;

  public DescriptorBasedProtobufBytesDecoder(
      @Nullable final String protoMessageType
  )
  {
    this.protoMessageType = protoMessageType;
  }

  @JsonProperty
  @Nullable
  public String getProtoMessageType()
  {
    return protoMessageType;
  }

  public Descriptors.Descriptor getDescriptor()
  {
    return descriptor;
  }

  @VisibleForTesting
  void initDescriptor()
  {
    if (this.descriptor == null) {
      final var descriptorSet = generateFileDescriptorSet();
      this.descriptor = generateDescriptor(descriptorSet);
    }
  }

  /**
   * Uses the generated descriptor to parse a message from the byte stream.
   */
  @Override
  public DynamicMessage parse(ByteBuffer bytes)
  {
    if (descriptor == null) {
      throw new IAE("Descriptor needs to be initialized before parsing");
    }

    try {
      return DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(bytes));
    }
    catch (Exception e) {
      throw new ParseException(
          null,
          e,
          "Failed to decode protobuf messagewith the provided descriptor [%s]",
          descriptor.getFullName()
      );
    }
  }

  protected abstract DescriptorProtos.FileDescriptorSet generateFileDescriptorSet();

  /**
   * Build a descriptor from a FileDescriptorSet.
   */
  protected Descriptors.Descriptor generateDescriptor(
      final DescriptorProtos.FileDescriptorSet descriptorSet
  )
  {
    try {
      // Build descriptors with dependency resolution - let protobuf handle well-known types automatically
      final var builtDescriptors = new HashMap<String, Descriptors.FileDescriptor>();
      final var userDescriptors = new ArrayList<Descriptors.FileDescriptor>();
      
      for (final var fileProto : descriptorSet.getFileList()) {
        final var fileDescriptor = buildFileDescriptor(fileProto, descriptorSet, builtDescriptors);
        userDescriptors.add(fileDescriptor);
      }

      // Find the target message type - only from user descriptors, not known deps
      if (protoMessageType == null) {
        // Return first message type found from user descriptors
        for (final var fileDescriptor : userDescriptors) {
          if (!fileDescriptor.getMessageTypes().isEmpty()) {
            return fileDescriptor.getMessageTypes().get(0);
          }
        }
        throw new ParseException(null, "No message types found in the descriptor set.");
      }

      // Find specific message type by name (including nested types)
      for (final var fileDescriptor : userDescriptors) {
        final var desc = findMessageByName(fileDescriptor, protoMessageType);
        if (desc != null) {
          return desc;
        }
      }

      throw new ParseException(
          null, 
          StringUtils.format("Protobuf message type '%s' not found in the descriptor set.", protoMessageType)
      );
    }
    catch (Descriptors.DescriptorValidationException e) {
      throw new ParseException(null, e, "Invalid protobuf descriptor");
    }
  }

  @Nullable
  private Descriptors.Descriptor findMessageByName(final Descriptors.FileDescriptor fileDescriptor, final String messageTypeName)
  {
    // Try simple name match first
    for (final var messageType : fileDescriptor.getMessageTypes()) {
      if (messageType.getName().equals(messageTypeName) || messageType.getFullName().equals(messageTypeName)) {
        return messageType;
      }
      
      // Try nested types (e.g., "ParentMessage.NestedMessage")
      final var nested = findNestedMessage(messageType, messageTypeName);
      if (nested != null) {
        return nested;
      }
    }
    return null;
  }

  @Nullable
  private Descriptors.Descriptor findNestedMessage(final Descriptors.Descriptor parent, final String messageTypeName)
  {
    for (final var nested : parent.getNestedTypes()) {
      if (nested.getName().equals(messageTypeName) || 
          nested.getFullName().equals(messageTypeName) ||
          (parent.getName() + "." + nested.getName()).equals(messageTypeName)) {
        return nested;
      }
      
      // Recursively search deeper nesting
      final var deeperNested = findNestedMessage(nested, messageTypeName);
      if (deeperNested != null) {
        return deeperNested;
      }
    }
    return null;
  }

  private Descriptors.FileDescriptor buildFileDescriptor(
      final DescriptorProtos.FileDescriptorProto fileProto,
      final DescriptorProtos.FileDescriptorSet descriptorSet,
      final Map<String, Descriptors.FileDescriptor> builtDescriptors
  ) throws Descriptors.DescriptorValidationException
  {
    // Return if already built
    if (builtDescriptors.containsKey(fileProto.getName())) {
      return builtDescriptors.get(fileProto.getName());
    }

    // Collect dependencies from the descriptor set (protobuf library handles well-known types automatically)
    final var dependencies = new ArrayList<Descriptors.FileDescriptor>();
    for (final var dependencyName : fileProto.getDependencyList()) {
      // Look for the dependency in the descriptor set
      for (final var depProto : descriptorSet.getFileList()) {
        if (depProto.getName().equals(dependencyName)) {
          final var depDescriptor = buildFileDescriptor(depProto, descriptorSet, builtDescriptors);
          dependencies.add(depDescriptor);
          break;
        }
      }
    }

    // Build the file descriptor - protobuf library automatically handles well-known types
    final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(
        fileProto, 
        dependencies.toArray(new Descriptors.FileDescriptor[0])
    );
    
    builtDescriptors.put(fileProto.getName(), fileDescriptor);
    return fileDescriptor;
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
    DescriptorBasedProtobufBytesDecoder that = (DescriptorBasedProtobufBytesDecoder) o;
    return Objects.equals(getProtoMessageType(), that.getProtoMessageType());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getProtoMessageType());
  }
}
