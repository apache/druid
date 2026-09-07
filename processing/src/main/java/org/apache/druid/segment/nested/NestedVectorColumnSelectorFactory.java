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

package org.apache.druid.segment.nested;

import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.util.List;

public interface NestedVectorColumnSelectorFactory
{
  /**
   * Make a {@link SingleValueDimensionVectorSelector} for a nested field column
   *
   * @param path                  nested path, or empty list to read the root
   * @param columnSelectorFactory factory for underlying selectors, if needed
   * @param readableOffset        offset for the selector
   */
  SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      List<NestedPathPart> path,
      VectorColumnSelectorFactory columnSelectorFactory,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link VectorObjectSelector} for a nested field column.
   *
   * @param path                  nested path, or empty list to read the root
   * @param columnSelectorFactory factory for underlying selectors, if needed
   * @param readableOffset        offset for the selector
   */
  VectorObjectSelector makeVectorObjectSelector(
      List<NestedPathPart> path,
      VectorColumnSelectorFactory columnSelectorFactory,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link VectorValueSelector} for a nested field column
   *
   * @param path                  nested path, or empty list to read the root
   * @param columnSelectorFactory factory for underlying selectors, if needed
   * @param readableOffset        offset for the selector
   */
  VectorValueSelector makeVectorValueSelector(
      List<NestedPathPart> path,
      VectorColumnSelectorFactory columnSelectorFactory,
      ReadableVectorOffset readableOffset
  );
}
