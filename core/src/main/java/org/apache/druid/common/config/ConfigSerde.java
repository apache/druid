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

package org.apache.druid.common.config;

/**
*/
public interface ConfigSerde<T>
{
  byte[] serialize(T obj);
  /**
   * Serialize object to String
   *
   * @param obj to be serialize
   * @param skipNull if true, then skip serialization of any field with null value.
   *                 This can be used to reduce the size of the resulting String.
   * @return String serialization of the input
   */
  String serializeToString(T obj, boolean skipNull);
  T deserialize(byte[] bytes);
}
