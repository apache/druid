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

package org.apache.druid.query.filter.vector;

import javax.annotation.Nullable;

/**
 * The result of calling {@link VectorValueMatcher#match}.
 *
 * @see VectorMatch, the implementation, which also adds some extra mutation methods.
 */
public interface ReadableVectorMatch
{
  /**
   * Returns an array of indexes into the current batch. Only the first "getSelectionSize" are valid.
   *
   * Even though this array is technically mutable, it is very poor form to mutate it if you are not the owner of the
   * VectorMatch object. The reason we use a mutable array here instead of positional getter methods, by the way, is in
   * the hopes of keeping access to the selection vector as low-level and optimizable as possible. Potential
   * optimizations could include making it easier for the JVM to use CPU-level vectorization, avoid method calls, etc.
   */
  int[] getSelection();

  /**
   * Returns the number of valid values in the array from "getSelection".
   */
  int getSelectionSize();

  /**
   * Checks if this match has accepted every row in the vector.
   *
   * @param vectorSize the current vector size; must be passed in since VectorMatch objects do not "know" the size
   *                   of the vector they came from.
   */
  boolean isAllTrue(int vectorSize);

  /**
   * Checks if this match has accepted *nothing*.
   */
  boolean isAllFalse();

  /**
   * Checks if this match is valid (increasing row numbers, no out-of-range row numbers). Can additionally verify
   * that the match is a subset of a provided "mask".
   *
   * Used by assertions and tests.
   *
   * @param mask if provided, checks if this match is a subset of the mask.
   */
  boolean isValid(@Nullable ReadableVectorMatch mask);
}
