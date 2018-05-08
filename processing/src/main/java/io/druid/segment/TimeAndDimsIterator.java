/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import java.io.Closeable;

/**
 * TimeAndDimsIterator (in conjunction with {@link TimeAndDimsPointer}) is an {@link java.util.Iterator}-like
 * abstraction, designed for allocation-free transformation, merging, combining and iteration over a stream of data
 * points.
 *
 * Usage pattern:
 * try (TimeAndDimsIterator iterator = obtainNewTimeAndDimsIteratorFromSomewhere()) {
 *   while (iterator.moveToNext()) {
 *     TimeAndDimsPointer pointer = iterator.getPointer();
 *     doSomethingWithPointer(pointer);
 *   }
 * }
 */
public interface TimeAndDimsIterator extends Closeable
{
  /**
   * Moves iterator to the next data point. This method must be called before the first use of {@link #getPointer()}.
   * As long as this method returns {@code true}, {@link #getPointer()} could be safely called; after this method
   * returned {@code false}, this iterator is done, {@link #getPointer()} must _not_ be called, and {@link #close()}
   * should be called.
   */
  boolean moveToNext();

  /**
   * Returns a pointer to the current data point. This method may return the same (in terms of referencial identity),
   * as well as different object on any calls, but the data point itself, to which the returned object points, changes
   * after each {@link #moveToNext()} call that returns {@link true}.
   *
   * This method must not be called before ever calling to {@link #moveToNext()}. After a call to {@link #moveToNext()}
   * returned {@code false}, the behaviour of this method is undefined (it may return a "wrong" pointer, null,
   * throw an exception, etc.)
   */
  TimeAndDimsPointer getPointer();

  /**
   * Closes any resources, associated with this iterator.
   */
  @Override
  void close();
}
