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
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;

/**
 * Represents "absent" column.
 */
@PublicApi
public class NilColumnValueSelector implements ColumnValueSelector
{
  private static final NilColumnValueSelector INSTANCE = NullHandling.sqlCompatible()
                                                         ? new SqlCompatibleNilColumnValueSelector()
                                                         : new NilColumnValueSelector();

  public static NilColumnValueSelector instance()
  {
    return INSTANCE;
  }

  private NilColumnValueSelector()
  {
  }

  /**
   * always returns 0, if {@link NullHandling#replaceWithDefault} is set to true,
   * or always throws an exception, if {@link NullHandling#replaceWithDefault} is
   * set to false.
   */
  @Override
  public double getDouble()
  {
    return 0.0;
  }

  /**
   * always returns 0.0f, if {@link NullHandling#replaceWithDefault} is set to true,
   * or always throws an exception, if {@link NullHandling#replaceWithDefault} is
   * set to false.
   */
  @Override
  public float getFloat()
  {
    return 0.0f;
  }

  /**
   * always returns 0L, if {@link NullHandling#replaceWithDefault} is set to true,
   * or always throws an exception, if {@link NullHandling#replaceWithDefault} is
   * set to false.
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
  public boolean isNull()
  {
    return true;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }

  private static class SqlCompatibleNilColumnValueSelector extends NilColumnValueSelector
  {
    @Override
    public double getDouble()
    {
      throw new IllegalStateException("Cannot return null value as double");
    }

    @Override
    public float getFloat()
    {
      throw new IllegalStateException("Cannot return null value as float");
    }
    
    @Override
    public long getLong()
    {
      throw new IllegalStateException("Cannot return null value as long");
    }

  }
}
