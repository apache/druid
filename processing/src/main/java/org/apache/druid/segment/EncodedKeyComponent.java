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

package org.apache.druid.segment;

import javax.annotation.Nullable;

/**
 * Represents the encoded component of a row key corresponding to a single dimension.
 * The row key contains a component for each dimension.
 *
 * @param <K> Encoded key component type
 */
public class EncodedKeyComponent<K>
{
  @Nullable
  private final K component;
  private final long effectiveSizeBytes;

  /**
   * Creates an EncodedKeyComponent corresponding to a single dimension.
   *
   * @param component          The encoded dimension value(s)
   * @param effectiveSizeBytes Effective size of the key component in bytes. This
   *                           value is used to estimate on-heap memory usage and
   *                           must account for the footprint of both the original
   *                           and encoded dimension values, as applicable.
   */
  public EncodedKeyComponent(@Nullable K component, long effectiveSizeBytes)
  {
    this.component = component;
    this.effectiveSizeBytes = effectiveSizeBytes;
  }

  /**
   * Encoded dimension value(s) to be used as a component for a row key.
   */
  @Nullable
  public K getComponent()
  {
    return component;
  }

  /**
   * Effective size of the key component in bytes. This value is used to estimate
   * on-heap memory usage and accounts for the memory footprint of both the
   * original and encoded dimension values, as applicable.
   */
  public long getEffectiveSizeBytes()
  {
    return effectiveSizeBytes;
  }
}
