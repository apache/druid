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

import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.virtual.BaseSingleValueDimensionSelector;

public class DoubleWrappingDimensionSelector extends BaseSingleValueDimensionSelector
{

  private final ExtractionFn extractionFn;
  private final BaseDoubleColumnValueSelector selector;

  public DoubleWrappingDimensionSelector(BaseDoubleColumnValueSelector doubleColumnSelector, ExtractionFn extractionFn)
  {
    this.extractionFn = extractionFn;
    selector = doubleColumnSelector;
  }

  @Override
  protected String getValue()
  {
    if (extractionFn == null) {
      return String.valueOf(selector.getDouble());
    } else {
      return extractionFn.apply(selector.getDouble());
    }
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("extractionFn", extractionFn);
  }
}
