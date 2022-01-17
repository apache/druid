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
 * <p>
 * Contains:
 * <ul>
 *   <li>the encoded dimension value(s)</li>
 *    <li>the increase in size (in bytes) caused by adding the dimension value(s)</li>
 * </ul>
 *
 * @param <K> Encoded key component type
 */
public class EncodedKeyComponent<K>
{
  @Nullable
  private final K component;
  private final long incrementalSizeBytes;

  EncodedKeyComponent(@Nullable K component, long incrementalSizeBytes)
  {
    this.component = component;
    this.incrementalSizeBytes = incrementalSizeBytes;
  }

  /**
   * The encoded dimension value(s) to be used a component for a row key.
   */
  @Nullable
  public K getComponent()
  {
    return component;
  }

  /**
   * Increase in size (in bytes) caused by adding the dimension value(s).
   */
  public long getIncrementalSizeBytes()
  {
    return incrementalSizeBytes;
  }
}
