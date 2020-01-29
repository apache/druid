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

import java.io.Closeable;

/**
 * An interface to reference-counted objects. Used by {@link ReferenceCountingSegment}. Thread-safe.
 */
public interface ReferenceCounter
{
  /**
   * Increment the reference count by one.
   */
  boolean increment();

  /**
   * Returns a {@link Closeable} which action is to call {@link #decrement()} only once. If close() is called on the
   * returned Closeable object for the second time, it won't call {@link #decrement()} again.
   */
  Closeable decrementOnceCloseable();

  /**
   * Decrement the reference count by one.
   */
  void decrement();
}
