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

package org.apache.druid.compressedbigdecimal;


import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public abstract class CompressedBigDecimalAggregateCombinerBase implements AggregateCombiner<CompressedBigDecimal>
{
  protected CompressedBigDecimal value;

  private final String className;

  protected CompressedBigDecimalAggregateCombinerBase(String className)
  {
    this.className = className;
  }

  @Override
  public abstract void reset(@SuppressWarnings("rawtypes") ColumnValueSelector columnValueSelector);

  @Override
  public abstract void fold(@SuppressWarnings("rawtypes") ColumnValueSelector columnValueSelector);

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException(className + " does not support getDouble()");
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException(className + " does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException(className + " does not support getLong()");
  }

  @Nullable
  @Override
  public CompressedBigDecimal getObject()
  {
    return value;
  }

  @Override
  public Class<CompressedBigDecimal> classOfObject()
  {
    return CompressedBigDecimal.class;
  }
}
