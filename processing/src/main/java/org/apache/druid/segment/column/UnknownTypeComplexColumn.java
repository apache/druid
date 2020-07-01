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

package org.apache.druid.segment.column;

import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

public class UnknownTypeComplexColumn implements ComplexColumn
{
  private static final UnknownTypeComplexColumn INSTANCE = new UnknownTypeComplexColumn();

  public static UnknownTypeComplexColumn instance()
  {
    return INSTANCE;
  }

  @Override
  public Class<?> getClazz()
  {
    return ComplexColumn.class;
  }

  @Override
  public String getTypeName()
  {
    return "UNKNOWN_COMPLEX_COLUMN_TYPE";
  }

  @Nullable
  @Override
  public Object getRowValue(int rowNum)
  {
    return null;
  }

  @Override
  public int getLength()
  {
    return 0;
  }

  @Override
  public void close()
  {

  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return NilColumnValueSelector.instance();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    return NilVectorSelector.create(offset);
  }
}
