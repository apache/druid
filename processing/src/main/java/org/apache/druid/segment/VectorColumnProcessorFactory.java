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

import com.google.common.base.Preconditions;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
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
   * Called only if {@link ColumnCapabilities#getType()} is STRING and the underlying column always has a single value
   * per row.
   *
   * Note that for STRING-typed columns where the dictionary does not exist or is not expected to be useful,
   * {@link #makeObjectProcessor} may be called instead. To handle all string inputs properly, processors must implement
   * all three methods (single-value, multi-value, object).
   */
  T makeSingleValueDimensionProcessor(
      ColumnCapabilities capabilities,
      SingleValueDimensionVectorSelector selector
  );

  /**
   * Called only if {@link ColumnCapabilities#getType()} is STRING and the underlying column may have multiple values
   * per row.
   *
   * Note that for STRING-typed columns where the dictionary does not exist or is not expected to be useful,
   * {@link #makeObjectProcessor} may be called instead. To handle all string inputs properly, processors must implement
   * all three methods (single-value, multi-value, object).
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
   * Called when {@link ColumnCapabilities#getType()} is ARRAY.
   */
  T makeArrayProcessor(ColumnCapabilities capabilities, VectorObjectSelector selector);

  /**
   * Called when {@link ColumnCapabilities#getType()} is COMPLEX. May also be called for STRING typed columns in
   * cases where the dictionary does not exist or is not expected to be useful.
   *
   * @see VectorObjectSelector#getObjectVector() for details on what can appear here when type is STRING
   */
  T makeObjectProcessor(@SuppressWarnings("unused") ColumnCapabilities capabilities, VectorObjectSelector selector);

  /**
   * The processor factory can influence the decision on whether or not to prefer a dictionary encoded column value
   * selector over a an object selector by examining the {@link ColumnCapabilities}.
   *
   * By default, all processor factories prefer to use a dictionary encoded selector if the column has a dictionary
   * available ({@link ColumnCapabilities#isDictionaryEncoded()} is true), and there is a unique mapping of dictionary
   * id to value ({@link ColumnCapabilities#areDictionaryValuesUnique()} is true), but this can be overridden
   * if there is more appropriate behavior for a given processor.
   *
   * For processors, this means by default only actual dictionary encoded string columns (likely from real segments)
   * will use {@link SingleValueDimensionVectorSelector} and {@link MultiValueDimensionVectorSelector}, while
   * processors on things like string expression virtual columns will prefer to use {@link VectorObjectSelector}. In
   * other words, it is geared towards use cases where there is a clear opportunity to benefit to deferring having to
   * deal with the actual string value in exchange for the increased complexity of dealing with dictionary encoded
   * selectors.
   */
  default boolean useDictionaryEncodedSelector(ColumnCapabilities capabilities)
  {
    Preconditions.checkArgument(capabilities != null, "Capabilities must not be null");
    Preconditions.checkArgument(capabilities.is(ValueType.STRING), "Must only be called on a STRING column");
    return capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue();
  }
}
