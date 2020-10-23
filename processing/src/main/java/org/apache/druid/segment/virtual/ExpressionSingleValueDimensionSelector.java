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

package org.apache.druid.segment.virtual;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

class ExpressionSingleValueDimensionSelector extends BaseSingleValueDimensionSelector
{
  public static ExpressionSingleValueDimensionSelector fromValueSelector(
      ColumnValueSelector<ExprEval> baseSelector,
      @Nullable ExtractionFn extractionFn
  )
  {
    if (extractionFn != null) {
      return new ExtractionExpressionDimensionSelector(baseSelector, extractionFn);
    }
    return new ExpressionSingleValueDimensionSelector(baseSelector);
  }

  protected final ColumnValueSelector<ExprEval> baseSelector;

  ExpressionSingleValueDimensionSelector(ColumnValueSelector<ExprEval> baseSelector)
  {
    this.baseSelector = baseSelector;
  }

  @Override
  protected String getValue()
  {
    return NullHandling.emptyToNullIfNeeded(baseSelector.getObject().asString());
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("baseSelector", baseSelector);
  }


  /**
   * expressions + extractions
   */
  static class ExtractionExpressionDimensionSelector extends ExpressionSingleValueDimensionSelector
  {
    private final ExtractionFn extractionFn;

    ExtractionExpressionDimensionSelector(ColumnValueSelector<ExprEval> baseSelector, ExtractionFn extractionFn)
    {
      super(baseSelector);
      this.extractionFn = extractionFn;
    }

    @Override
    protected String getValue()
    {
      return extractionFn.apply(NullHandling.emptyToNullIfNeeded(baseSelector.getObject().asString()));
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseSelector", baseSelector);
      inspector.visit("extractionFn", extractionFn);
    }
  }
}
