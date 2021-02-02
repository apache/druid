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

import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * Class that encapsulates knowledge about how to create vector column processors. Used by
 * {@link ColumnProcessors#makeVectorProcessor}.
 *
 * Column processors can be any type "T". The idea is that a ColumnProcessorFactory embodies the logic for wrapping
 * and processing selectors of various types, and so enables nice code design, where type-dependent code is not
 * sprinkled throughout.
 *
 * Unlike {@link ColumnProcessorFactory}, this interface does not have a "defaultType" method, because vector
 * column types are always known, so it isn't necessary.
 *
 * @see ColumnProcessorFactory the non-vectorized version
 */
public interface VectorColumnProcessorFactory<T>
{
  /**
   * Called when {@link ColumnCapabilities#getType()} is STRING and the underlying column always has a single value
   * per row.
   */
  T makeSingleValueDimensionProcessor(
      ColumnCapabilities capabilities,
      SingleValueDimensionVectorSelector selector
  );

  /**
   * Called when {@link ColumnCapabilities#getType()} is STRING and the underlying column may have multiple values
   * per row.
   */
  T makeMultiValueDimensionProcessor(
      ColumnCapabilities capabilities,
      MultiValueDimensionVectorSelector selector
  );

  /**
   * Called when {@link ColumnCapabilities#getType()} is FLOAT.
   */
  T makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector);

  /**
   * Called when {@link ColumnCapabilities#getType()} is DOUBLE.
   */
  T makeDoubleProcessor(ColumnCapabilities capabilities, VectorValueSelector selector);

  /**
   * Called when {@link ColumnCapabilities#getType()} is LONG.
   */
  T makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector);

  /**
   * Called when {@link ColumnCapabilities#getType()} is COMPLEX.
   */
  T makeObjectProcessor(@SuppressWarnings("unused") ColumnCapabilities capabilities, VectorObjectSelector selector);
}
