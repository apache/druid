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

package org.apache.druid.java.util.common.parsers;

import java.util.Map;

public interface ObjectFlattener<T>
{
  /**
   * Transforms an input row object into a {@link Map}, likely based on the instructions in some {@link JSONPathSpec}.
   *
   * This method is used in normal ingestion to extract values into a map to translate into an
   * {@link org.apache.druid.data.input.InputRow}
   */
  Map<String, Object> flatten(T obj);

  /**
   * Completely transforms an input row into a {@link Map}, including translating all nested structure into plain java
   * objects such as {@link Map} and {@link java.util.List}. This method should translate everything as much as
   * possible, ignoring any instructions in {@link JSONPathSpec} which might otherwise limit the amount of
   * transformation done.
   *
   * This method is used by the ingestion "sampler" to provide a "raw" JSON form of the original input data, regardless
   * of actual format, so that it can use "inline" JSON datasources and reduce sampling overhead.
   */
  Map<String, Object> toMap(T obj);
}
