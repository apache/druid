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

package org.apache.druid.grpc.client;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class GrpcResponseHandler<T extends AbstractMessageLite>
{
  private final T message;

  private GrpcResponseHandler(final Class<T> clazz)
  {
    this.message = get(clazz);
  }

  public static <T extends AbstractMessageLite> GrpcResponseHandler<T> of(Class<T> clazz)
  {
    return new GrpcResponseHandler<>(clazz);
  }

  public List<T> get(ByteString byteString)
  {
    return get(new ByteArrayInputStream(byteString.toByteArray()));
  }

  @SuppressWarnings("unchecked")
  public List<T> get(InputStream inputStream)
  {
    try {
      final List<T> data = new ArrayList<>();
      while (true) {
        try {
          final MessageLite messageLite =
                  message
                          .getDefaultInstanceForType()
                          .getParserForType()
                          .parseDelimitedFrom(inputStream);
          if (messageLite == null) {
            break;
          }
          data.add((T) messageLite);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return data;
    }
    finally {
      try {
        inputStream.close();
      }
      catch (IOException e) {
        // ignore
      }
    }
  }

  @SuppressWarnings("unchecked")
  private T get(Class<T> clazz)
  {
    try {
      final Method method = clazz.getMethod("getDefaultInstance", new Class<?>[0]);
      return (T) method.invoke(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
