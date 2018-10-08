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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;

/**
*/
public class ComplexColumn implements BaseColumn
{
  private final GenericIndexed<?> index;
  private final String typeName;

  public ComplexColumn(String typeName, GenericIndexed<?> index)
  {
    this.index = index;
    this.typeName = typeName;
  }

  public String getTypeName()
  {
    return typeName;
  }

  @Nullable
  public Object getRowValue(int rowNum)
  {
    return index.get(rowNum);
  }

  public int getLength()
  {
    return index.size();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        return getRowValue(offset.getOffset());
      }

      @Override
      public Class classOfObject()
      {
        return index.getClazz();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", ComplexColumn.this);
      }
    };
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
