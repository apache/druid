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

package org.apache.druid.grpc.server;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@code ResultFormat.Writer} for protobuf message.
 */
public class ProtobufWriter implements ResultFormat.Writer
{
  private final OutputStream outputStream;
  private final GeneratedMessageV3 message;
  private Message.Builder rowBuilder;
  private final Map<String, Method> methods = new HashMap<>();


  public ProtobufWriter(OutputStream outputStream, Class<GeneratedMessageV3> clazz)
  {
    this.outputStream = outputStream;
    this.message = get(clazz);
  }

  private GeneratedMessageV3 get(Class<GeneratedMessageV3> clazz)
  {
    try {
      final Method method = clazz.getMethod("getDefaultInstance", new Class<?>[0]);
      return clazz.cast(method.invoke(null));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeResponseStart()
  {
  }

  @Override
  public void writeHeader(RelDataType rowType, boolean includeTypes, boolean includeSqlTypes)
  {
  }

  @Override
  public void writeHeaderFromRowSignature(RowSignature rowSignature, boolean b)
  {

  }

  @Override
  public void writeRowStart()
  {
    rowBuilder = message.getDefaultInstanceForType().newBuilderForType();
  }

  @Override
  public void writeRowField(String name, @Nullable Object value)
  {
    if (value == null) {
      return;
    }
    final Descriptors.FieldDescriptor fieldDescriptor =
        message.getDescriptorForType().findFieldByName(name);
    // we should throw an exception if fieldDescriptor is null
    // this means the .proto fields don't match returned column names
    if (fieldDescriptor == null) {
      throw new QueryDriver.RequestError(
          "Field [%s] not found in Protobuf [%s]",
          name,
          message.getClass()
      );
    }
    final Method method = methods.computeIfAbsent("setField", k -> {
      try {
        return rowBuilder
            .getClass()
            .getMethod(
                "setField", new Class<?>[]{Descriptors.FieldDescriptor.class, Object.class});
      }
      catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    });
    try {
      method.invoke(rowBuilder, fieldDescriptor, value);
    }
    catch (IllegalAccessException | InvocationTargetException e) {
      throw new QueryDriver.RequestError(
          "Could not write value [%s] to field [%s]",
          value,
          name
      );
    }
  }

  @Override
  public void writeRowEnd() throws IOException
  {
    Message rowMessage = rowBuilder.build();
    rowMessage.writeDelimitedTo(outputStream);
  }

  @Override
  public void writeResponseEnd()
  {
  }

  @Override
  public void close() throws IOException
  {
    outputStream.flush();
    outputStream.close();
  }
}
