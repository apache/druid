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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.Value;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Convert {@link Message} to plain java stuffs, based roughly on the conversions done with {@link JsonFormat}
 */
public class ProtobufConverter
{
  private static final Map<String, SpecializedConverter> SPECIAL_CONVERSIONS = buildSpecializedConversions();

  @Nullable
  public static Map<String, Object> convertMessage(Message msg) throws InvalidProtocolBufferException
  {
    if (msg == null) {
      return null;
    }
    final Map<Descriptors.FieldDescriptor, Object> fields = msg.getAllFields();
    final Map<String, Object> converted = Maps.newHashMapWithExpectedSize(fields.size());
    for (Map.Entry<Descriptors.FieldDescriptor, Object> field : fields.entrySet()) {
      converted.put(field.getKey().getJsonName(), convertField(field.getKey(), field.getValue()));
    }
    return converted;
  }

  @Nullable
  private static Object convertField(Descriptors.FieldDescriptor field, Object value)
      throws InvalidProtocolBufferException
  {
    // handle special types
    if (value instanceof Message) {
      Message msg = (Message) value;
      final String typeName = msg.getDescriptorForType().getFullName();
      SpecializedConverter converter = SPECIAL_CONVERSIONS.get(typeName);
      if (converter != null) {
        return converter.convert(msg);
      }
    }

    if (field.isMapField()) {
      return convertMap(field, value);
    } else if (field.isRepeated()) {
      return convertList(field, (List<?>) value);
    } else {
      return convertSingleValue(field, value);
    }
  }

  @Nonnull
  private static List<Object> convertList(Descriptors.FieldDescriptor field, List<?> value)
      throws InvalidProtocolBufferException
  {
    final List<Object> theList = Lists.newArrayListWithExpectedSize(value.size());
    for (Object element : value) {
      theList.add(convertSingleValue(field, element));
    }
    return theList;
  }

  @Nullable
  private static Object convertMap(Descriptors.FieldDescriptor field, Object value)
      throws InvalidProtocolBufferException
  {
    final Descriptors.Descriptor type = field.getMessageType();
    final Descriptors.FieldDescriptor keyField = type.findFieldByName("key");
    final Descriptors.FieldDescriptor valueField = type.findFieldByName("value");
    if (keyField == null || valueField == null) {
      throw new InvalidProtocolBufferException("Invalid map field.");
    }

    @SuppressWarnings("unchecked")
    final List<Object> elements = (List<Object>) value;
    final HashMap<String, Object> theMap = Maps.newHashMapWithExpectedSize(elements.size());
    for (Object element : elements) {
      Message entry = (Message) element;
      theMap.put(
          (String) convertSingleValue(keyField, entry.getField(keyField)),
          convertSingleValue(valueField, entry.getField(valueField))
      );
    }
    return theMap;
  }

  @Nullable
  private static Object convertSingleValue(Descriptors.FieldDescriptor field, Object value)
      throws InvalidProtocolBufferException
  {
    switch (field.getType()) {
      case BYTES:
        return ((ByteString) value).toByteArray();
      case ENUM:
        if ("google.protobuf.NullValue".equals(field.getEnumType().getFullName())) {
          return null;
        } else {
          return ((Descriptors.EnumValueDescriptor) value).getName();
        }
      case MESSAGE:
      case GROUP:
        return convertMessage((Message) value);
      default:
        // pass through everything else
        return value;
    }
  }

  private static Map<String, SpecializedConverter> buildSpecializedConversions()
  {
    final Map<String, SpecializedConverter> converters = new HashMap<>();
    final SpecializedConverter parappaTheWrappa = msg -> {
      final Descriptors.Descriptor descriptor = msg.getDescriptorForType();
      final Descriptors.FieldDescriptor valueField = descriptor.findFieldByName("value");
      if (valueField == null) {
        throw new InvalidProtocolBufferException("Invalid Wrapper type.");
      }
      return convertSingleValue(valueField, msg.getField(valueField));
    };
    converters.put(BoolValue.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(Int32Value.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(UInt32Value.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(Int64Value.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(UInt64Value.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(StringValue.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(BytesValue.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(FloatValue.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(DoubleValue.getDescriptor().getFullName(), parappaTheWrappa);
    converters.put(
        Any.getDescriptor().getFullName(),
        msg -> JsonFormat.printer().print(msg) // meh
    );
    converters.put(
        Timestamp.getDescriptor().getFullName(),
        msg -> {
          final Timestamp ts = Timestamp.parseFrom(msg.toByteString());
          return Timestamps.toString(ts);
        }
    );
    converters.put(
        Duration.getDescriptor().getFullName(),
        msg -> {
          final Duration duration = Duration.parseFrom(msg.toByteString());
          return Durations.toString(duration);
        }
    );
    converters.put(
        FieldMask.getDescriptor().getFullName(),
        msg -> FieldMaskUtil.toJsonString(FieldMask.parseFrom(msg.toByteString()))
    );
    converters.put(
        Struct.getDescriptor().getFullName(),
        msg -> {
          final Descriptors.Descriptor descriptor = msg.getDescriptorForType();
          final Descriptors.FieldDescriptor field = descriptor.findFieldByName("fields");
          if (field == null) {
            throw new InvalidProtocolBufferException("Invalid Struct type.");
          }
          // Struct is formatted as a map object.
          return convertSingleValue(field, msg.getField(field));
        }
    );
    converters.put(
        Value.getDescriptor().getFullName(),
        msg -> {
          final Map<Descriptors.FieldDescriptor, Object> fields = msg.getAllFields();
          if (fields.isEmpty()) {
            return null;
          }
          if (fields.size() != 1) {
            throw new InvalidProtocolBufferException("Invalid Value type.");
          }
          final Map.Entry<Descriptors.FieldDescriptor, Object> entry = fields.entrySet().stream().findFirst().get();
          return convertSingleValue(entry.getKey(), entry.getValue());
        }
    );
    converters.put(
        ListValue.getDescriptor().getFullName(),
        msg -> {
          Descriptors.Descriptor descriptor = msg.getDescriptorForType();
          Descriptors.FieldDescriptor field = descriptor.findFieldByName("values");
          if (field == null) {
            throw new InvalidProtocolBufferException("Invalid ListValue type.");
          }
          return convertList(field, (List<?>) msg.getField(field));
        }
    );
    return converters;
  }

  @FunctionalInterface
  interface SpecializedConverter
  {
    @Nullable
    Object convert(Message msg) throws InvalidProtocolBufferException;
  }
}
