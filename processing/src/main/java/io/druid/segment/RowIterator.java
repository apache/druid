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

/**
 * Extension of {@link TimeAndDimsIterator}, specialized for {@link RowPointer} instead of {@link TimeAndDimsPointer}.
 *
 * Also, RowIterator encapsulates tracking of data point changes between {@link #moveToNext()} calls via {@link #mark()}
 * and {@link #hasTimeAndDimsChangedSinceMark()} methods. This functionality is used in {@link
 * RowCombiningTimeAndDimsIterator}, and also internally in {@link MergingRowIterator} to reduce the number of
 * comparisons that this class makes. This functionality is added directly to {@link RowIterator} interface rather than
 * left to be implemented externally to this interface, because it's inefficient to do the latter with "generic"
 * RowIterator, because of {@link TimeAndDimsPointer} allocation-free design, that reuses objects. On the other hand,
 * some implementations of RowIterator allow to optimize {@link #mark()} and {@link #hasTimeAndDimsChangedSinceMark()}.
 */
public interface RowIterator extends TimeAndDimsIterator
{
  /**
   * "Memoizes" the data point, to which {@link #getPointer()} currently points. If the last call to {@link
   * #moveToNext()} returned {@code false}, the behaviour of this method is undefined, e. g. it may throw a runtime
   * exception.
   */
  void mark();

  /**
   * Compares the "memoized" data point from the last {@link #mark()} call with the data point, to which {@link
   * #getPointer()} currently points. Comparison is made in terms of {@link TimeAndDimsPointer#compareTo} contract.
   *
   * If {@link #mark()} has never been called, or the last call to {@link #moveToNext()} returned {@code false}, the
   * behaviour of this method is undefined: it may arbitrarily return true, or false, or throw a runtime exception.
   */
  boolean hasTimeAndDimsChangedSinceMark();

  /**
   * Returns a pointer to the current row. This method may return the same (in terms of referencial identity),
   * as well as different object on any calls, but the row itself, to which the returned object points, changes
   * after each {@link #moveToNext()} call that returns {@link true}.
   *
   * This method must not be called before ever calling to {@link #moveToNext()}. If the very first call to {@link
   * #moveToNext()} returned {@code false}, the behaviour of this method is undefined (it may return a "wrong" pointer,
   * null, throw an exception, etc.).
   *
   * If {@link #moveToNext()} returned {@code true} one or more times, and then eventually returned {@code false},
   * calling {@code getPointer()} after that should return a pointer pointing to the last valid row, as if {@code
   * getPointer()} was called before the last (unsuccessful) call to {@link #moveToNext()}. In other words, unsuccessful
   * {@link #moveToNext()} call doesn't "corrupt" the pointer. This property is used in {@link
   * RowCombiningTimeAndDimsIterator#moveToNext}.
   */
  @Override
  RowPointer getPointer();
}
