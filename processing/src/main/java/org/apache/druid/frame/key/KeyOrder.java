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

package org.apache.druid.frame.key;

/**
 * Ordering associated with a {@link KeyColumn}.
 */
public enum KeyOrder
{
  /**
   * Not ordered.
   *
   * Possible if the key is used only for non-sorting purposes, such as hashing without sorting.
   */
  NONE(false),

  /**
   * Ordered ascending.
   *
   * Note that sortable key order does not necessarily mean that we are using range-based partitioning. We may be
   * using hash-based partitioning along with each partition internally being sorted by a key.
   */
  ASCENDING(true),

  /**
   * Ordered descending.
   *
   * Note that sortable key order does not necessarily mean that we are using range-based partitioning. We may be
   * using hash-based partitioning along with each partition internally being sorted by a key.
   */
  DESCENDING(true);

  private final boolean sortable;

  KeyOrder(boolean sortable)
  {
    this.sortable = sortable;
  }

  public boolean sortable()
  {
    return sortable;
  }
}
