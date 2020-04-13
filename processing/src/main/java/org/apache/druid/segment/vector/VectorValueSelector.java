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

package org.apache.druid.segment.vector;

import javax.annotation.Nullable;

/**
 * Vectorized selector for primitive columns.
 *
 * @see org.apache.druid.segment.ColumnValueSelector, the non-vectorized version.
 */
public interface VectorValueSelector extends VectorSizeInspector
{
  /**
   * Get the current vector, casting to longs as necessary. The array will be reused, so it is not a good idea to
   * retain a reference to it.
   */
  long[] getLongVector();

  /**
   * Get the current vector, casting to floats as necessary. The array will be reused, so it is not a good idea to
   * retain a reference to it.
   */
  float[] getFloatVector();

  /**
   * Get the current vector, casting to doubles as necessary. The array will be reused, so it is not a good idea to
   * retain a reference to it.
   */
  double[] getDoubleVector();

  /**
   * Gets a vector of booleans signifying which rows are null and which are not (true for null). Returns null if it is
   * known that there are no nulls in the vector, possibly because the column is non-nullable.
   */
  @Nullable
  boolean[] getNullVector();
}
