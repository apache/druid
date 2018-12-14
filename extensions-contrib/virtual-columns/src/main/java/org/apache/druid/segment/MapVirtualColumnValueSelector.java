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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

abstract class MapVirtualColumnValueSelector<T> implements ColumnValueSelector<T>
{
  private final DimensionSelector keySelector;
  private final DimensionSelector valueSelector;

  MapVirtualColumnValueSelector(DimensionSelector keySelector, DimensionSelector valueSelector)
  {
    this.keySelector = keySelector;
    this.valueSelector = valueSelector;
  }

  @Override
  public double getDouble()
  {
    assert NullHandling.replaceWithDefault();
    return 0.0;
  }

  @Override
  public float getFloat()
  {
    assert NullHandling.replaceWithDefault();
    return 0.0f;
  }

  @Override
  public long getLong()
  {
    assert NullHandling.replaceWithDefault();
    return 0L;
  }

  @Override
  public boolean isNull()
  {
    return false;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("keySelector", keySelector);
    inspector.visit("valueSelector", valueSelector);
  }
}
