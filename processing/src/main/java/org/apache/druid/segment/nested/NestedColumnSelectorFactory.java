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

import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;
import java.util.List;

public interface NestedColumnSelectorFactory
{
  /**
   * Make a {@link DimensionSelector} for a nested column.
   *
   * @param path                  nested path, or empty list to read the root
   * @param extractionFn          extraction fn to apply in the selector
   * @param columnSelectorFactory factory for underlying selectors, if needed
   * @param readableOffset        offset for the selector, if available
   */
  DimensionSelector makeDimensionSelector(
      List<NestedPathPart> path,
      @Nullable ExtractionFn extractionFn,
      ColumnSelectorFactory columnSelectorFactory,
      @Nullable ReadableOffset readableOffset
  );

  /**
   * Make a {@link ColumnValueSelector} for a nested column.
   *
   * @param path                  nested path, or empty list to read the root. If empty, the selector
   *                              returns {@link StructuredData}
   * @param columnSelectorFactory factory for underlying selectors, if needed
   * @param readableOffset        offset for the selector, if available
   */
  ColumnValueSelector<?> makeColumnValueSelector(
      List<NestedPathPart> path,
      ColumnSelectorFactory columnSelectorFactory,
      @Nullable ReadableOffset readableOffset
  );
}
