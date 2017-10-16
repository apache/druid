/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.guice.annotations.PublicApi;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;

/**
 * Represents "absent" column.
 */
@PublicApi
public final class NilColumnValueSelector implements ColumnValueSelector
{
  private static final NilColumnValueSelector INSTANCE = new NilColumnValueSelector();

  public static NilColumnValueSelector instance()
  {
    return INSTANCE;
  }

  private NilColumnValueSelector() {}

  /**
   * Always returns 0.0.
   */
  @Override
  public double getDouble()
  {
    return 0.0;
  }

  /**
   * Always returns 0.0f.
   */
  @Override
  public float getFloat()
  {
    return 0.0f;
  }

  /**
   * Always returns 0L.
   */
  @Override
  public long getLong()
  {
    return 0L;
  }

  /**
   * Always returns null.
   */
  @Nullable
  @Override
  public Object getObject()
  {
    return null;
  }

  /**
   * Returns Object.class.
   */
  @Override
  public Class classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
