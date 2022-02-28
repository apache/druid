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

package org.apache.druid.data.input;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;

public class KafkaUtils
{
  /**
   * Creates a MethodHandle that – when invoked on a KafkaRecordEntity - returns the given header value
   * for the underlying KafkaRecordEntity
   *
   * The method handle is roughly equivalent to the following function
   *
   * (KafkaRecordEntity input) -> {
   *   Header h = input.getRecord().headers().lastHeader(header)
   *   if (h != null) {
   *     return h.value();
   *   } else {
   *     return null;
   *   }
   * }
   *
   * Since KafkaRecordEntity only exists in the kafka-indexing-service plugin classloader,
   * we need to look up the relevant classes in the classloader where the InputEntity was instantiated.
   *
   * The handle returned by this method should be cached for the classloader it was invoked with.
   *
   * If the lookup fails for whatever reason, the method handle will always return null;
   *
   * @param classLoader the kafka-indexing-service classloader
   * @param header the header value to look up
   * @return a MethodHandle
   */
  public static MethodHandle lookupGetHeaderMethod(ClassLoader classLoader, String header)
  {
    try {
      Class entityType = Class.forName("org.apache.druid.data.input.kafka.KafkaRecordEntity", true, classLoader);
      Class recordType = Class.forName("org.apache.kafka.clients.consumer.ConsumerRecord", true, classLoader);
      Class headersType = Class.forName("org.apache.kafka.common.header.Headers", true, classLoader);
      Class headerType = Class.forName("org.apache.kafka.common.header.Header", true, classLoader);

      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle nonNullTest = lookup.findStatic(Objects.class, "nonNull",
                                                   MethodType.methodType(boolean.class, Object.class)
      ).asType(MethodType.methodType(boolean.class, headerType));

      final MethodHandle getRecordMethod = lookup.findVirtual(
          entityType,
          "getRecord",
          MethodType.methodType(recordType)
      );
      final MethodHandle headersMethod = lookup.findVirtual(recordType, "headers", MethodType.methodType(headersType));
      final MethodHandle lastHeaderMethod = lookup.findVirtual(
          headersType,
          "lastHeader",
          MethodType.methodType(headerType, String.class)
      );
      final MethodHandle valueMethod = lookup.findVirtual(headerType, "value", MethodType.methodType(byte[].class));

      return MethodHandles.filterReturnValue(
          MethodHandles.filterReturnValue(
              MethodHandles.filterReturnValue(getRecordMethod, headersMethod),
              MethodHandles.insertArguments(lastHeaderMethod, 1, header)
          ),
          // return null byte array if header is not present
          MethodHandles.guardWithTest(
              nonNullTest,
              valueMethod,
              // match valueMethod signature by dropping the header instance argument
              MethodHandles.dropArguments(MethodHandles.constant(byte[].class, null), 0, headerType)
          )
      );
    }
    catch (ReflectiveOperationException e) {
      // if lookup fails in the classloader where the InputEntity is defined, then the source may not be
      // the kafka-indexing-service classloader, or method signatures did not match.
      // In that case we return a method handle always returning null
      return noopMethodHandle();
    }
  }

  static MethodHandle noopMethodHandle()
  {
    return MethodHandles.dropArguments(MethodHandles.constant(byte[].class, null), 0, InputEntity.class);
  }
}
