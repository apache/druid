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

package org.apache.druid.segment.shim;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

/**
 * Shim from {@link VectorObjectSelector} to {@link ColumnValueSelector} for a {@link ShimCursor}.
 */
public class ShimObjectColumnValueSelector extends ObjectColumnSelector<Object>
{
  private final ShimCursor cursor;
  private final ReadableVectorInspector vectorInspector;
  private final VectorObjectSelector vectorSelector;

  private Object[] objectVector;
  private int objectId = ReadableVectorInspector.NULL_ID;

  public ShimObjectColumnValueSelector(
      final ShimCursor cursor,
      final VectorObjectSelector vectorSelector
  )
  {
    this.cursor = cursor;
    this.vectorInspector = cursor.vectorColumnSelectorFactory.getReadableVectorInspector();
    this.vectorSelector = vectorSelector;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    populateObjectVector();
    return objectVector[cursor.currentIndexInVector];
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Don't bother.
  }

  private void populateObjectVector()
  {
    final int id = vectorInspector.getId();
    if (id != objectId) {
      objectVector = vectorSelector.getObjectVector();
      objectId = id;
    }
  }
}
