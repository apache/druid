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
 * TransformableRowIterator tightens {@link RowIterator#getPointer()} contract, that allows to transform iterated
 * rows without allocations on each iterations, and reuse the mechanics of the underlying iterator. See {@link
 * IndexMerger#toMergedIndexRowIterator} for an example.
 */
public interface TransformableRowIterator extends RowIterator
{
  /**
   * Returns a pointer to the current row. This method _always returns the same object_, but pointing to
   * different row after each {@link #moveToNext()} call that returns {@link true}.
   *
   * Other aspects of the behaviour of this method are the same as in the generic {@link RowIterator#getPointer()}
   * contract.
   */
  @Override
  RowPointer getPointer();

  /**
   * Returns a pointer to the row, that was the current row when {@link #mark()} was called for the last time. This
   * method always returns the same object, but pointing to different rows after each {@link #mark()} call.
   *
   * This method must not be called before ever calling to {@link #moveToNext()}. If the very first call to {@link
   * #moveToNext()} returned {@code false}, the behaviour of this method is undefined (it may return a "wrong" pointer,
   * null, throw an exception, etc.).
   *
   * This method could be called before the first call to {@link #mark()} (it should return the same object as it is
   * required to return after all subsequent {@link #mark()} calls), but the data of the returned pointer should not
   * be accessed before the first call to {@link #mark()}.
   *
   * This method is used in {@link RowCombiningTimeAndDimsIterator} implementation. getMarkedPointer() returns {@link
   * TimeAndDimsPointer} instead of {@link RowPointer} (like {@link #getPointer()}) merely because {@link
   * RowCombiningTimeAndDimsIterator} doesn't need this method to return anything more specific than {@link
   * TimeAndDimsPointer}.
   */
  TimeAndDimsPointer getMarkedPointer();
}
