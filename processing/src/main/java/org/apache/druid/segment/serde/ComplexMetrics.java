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

package org.apache.druid.segment.serde;

import javax.annotation.Nullable;

/**
 *  ComplexMetrics houses a mapping of serde names to affiliated ComplexMetricSerde objects.
 *
 *  This is deprecated, use {@link ComplexTypes} instead.
 */
@Deprecated
public class ComplexMetrics
{
  @Nullable
  public static ComplexMetricSerde getSerdeForType(String type)
  {
    return (ComplexMetricSerde) ComplexTypes.getSerdeForType(type);
  }

  /**
   * Register a serde name -> ComplexMetricSerde mapping.
   *
   * <p>
   * If the specified serde key string is already used and the supplied ComplexMetricSerde is not of the same
   * type as the existing value in the map for said key, an ISE is thrown.
   * </p>
   *
   * @param type The serde name used as the key in the map.
   * @param serde The ComplexMetricSerde object to be associated with the 'type' in the map.
   */
  public static void registerSerde(String type, ComplexMetricSerde serde)
  {
    ComplexTypes.registerSerde(type, serde);
  }

  /**
   * Unregister a serde name -> ComplexMetricSerde mapping.
   *
   * If the specified serde key string is not in use, does nothing.
   *
   * Only expected to be used in tests.
   */
  @SuppressWarnings("unused")
  public static void unregisterSerde(String type)
  {
    ComplexTypes.unregisterSerde(type);
  }
}
