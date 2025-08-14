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
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

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
  void initializeDescriptor()
  {
    if (this.descriptor == null) {
      final var descriptorSet = loadFileDescriptorSet();
      this.descriptor = buildMessageDescriptor(descriptorSet);
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
          "Failed to decode protobuf message with the provided descriptor [%s]",
          descriptor.getFullName()
      );
    }
  }

  /**
   * Load the FileDescriptorSet containing protobuf schema definitions from the concrete implementation's source.
   */
  protected abstract DescriptorProtos.FileDescriptorSet loadFileDescriptorSet();

  /**
   * Build a message descriptor from a FileDescriptorSet.
   */
  protected Descriptors.Descriptor buildMessageDescriptor(
      final DescriptorProtos.FileDescriptorSet descriptorSet
  )
  {
    try {
      // Build all descriptors with dependency resolution
      final var allDescriptors = new HashMap<String, Descriptors.FileDescriptor>();
      for (final var fileProto : descriptorSet.getFileList()) {
        buildFileDescriptor(fileProto, descriptorSet, allDescriptors);
      }

      // If protoMessageType has not been set, return the first message type found
      if (protoMessageType == null) {
        for (final var fileDescriptor : allDescriptors.values()) {
          if (!fileDescriptor.getMessageTypes().isEmpty()) {
            return fileDescriptor.getMessageTypes().get(0);
          }
        }
        throw new ParseException(null, "No message types found in the descriptor set");
      }

      // Otherwise, find the specific message type by name
      for (final var fileDescriptor : allDescriptors.values()) {
        final var desc = findMessageByName(fileDescriptor, protoMessageType);
        if (desc != null) {
          return desc;
        }
      }

      // Something went wrong... collect all available types for better error message
      final var availableTypes = new TreeSet<String>();
      for (final var fileDescriptor : allDescriptors.values()) {
        collectAvailableTypes(fileDescriptor, availableTypes);
      }

      throw new ParseException(
          null,
          "Protobuf message type [%s] not found in the descriptor set. Available types: %s",
          protoMessageType,
          availableTypes
      );
    }
    catch (Descriptors.DescriptorValidationException e) {
      throw new ParseException(null, e, "Invalid protobuf descriptor");
    }
  }

  @Nullable
  private Descriptors.Descriptor findMessageByName(
      final Descriptors.FileDescriptor fileDescriptor,
      final String messageTypeName
  )
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

    // Collect dependencies from the descriptor set
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

    final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(
        fileProto,
        dependencies.toArray(new Descriptors.FileDescriptor[0])
    );

    builtDescriptors.put(fileProto.getName(), fileDescriptor);

    return fileDescriptor;
  }

  private void collectAvailableTypes(
      final Descriptors.FileDescriptor fileDescriptor,
      final TreeSet<String> availableTypes
  )
  {
    for (final var messageType : fileDescriptor.getMessageTypes()) {
      // Add short name and full name
      availableTypes.add(messageType.getName());
      availableTypes.add(messageType.getFullName());

      // Add nested types
      collectNestedTypes(messageType, availableTypes);
    }
  }

  private void collectNestedTypes(final Descriptors.Descriptor parentDescriptor, final TreeSet<String> availableTypes)
  {
    for (final var nestedType : parentDescriptor.getNestedTypes()) {
      // Add different naming variations for nested types
      availableTypes.add(nestedType.getName());
      availableTypes.add(nestedType.getFullName());
      availableTypes.add(parentDescriptor.getName() + "." + nestedType.getName());

      // Recursively collect deeper nested types
      collectNestedTypes(nestedType, availableTypes);
    }
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
