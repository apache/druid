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

package org.apache.druid.segment.projections;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

public class ConstantTimeColumn implements NumericColumn
{
  public static Supplier<BaseColumnHolder> makeConstantTimeSupplier(int numRows, long constant)
  {
    return Suppliers.memoize(
        () -> new ColumnBuilder().setNumericColumnSupplier(() -> new ConstantTimeColumn(numRows, constant))
                                 .setColumnFormat(
                                     new CapabilitiesBasedFormat(
                                         ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG)
                                     )
                                 )
                                 .setType(ColumnType.LONG)
                                 .setHasNulls(false)
                                 .build()
    );
  }

  private final int numRows;
  private final long constant;

  public ConstantTimeColumn(int numRows, long constant)
  {
    this.numRows = numRows;
    this.constant = constant;
  }

  @Override
  public int length()
  {
    return numRows;
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
    return constant;
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {

  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ConstantExprEvalSelector(ExprEval.ofLong(constant));
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    return ConstantVectorSelectors.vectorValueSelector(offset, constant);
  }
}
