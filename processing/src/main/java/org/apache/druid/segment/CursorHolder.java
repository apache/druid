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

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.vector.VectorCursor;

import javax.annotation.Nullable;
import java.io.Closeable;

public interface CursorHolder extends Closeable
{
  /**
   * Create a {@link Cursor} for use with non-vectorized query engines.
   */
  @Nullable
  Cursor asCursor();

  /**
   * Create a {@link VectorCursor} for use with vectorized query engines.
   */
  @Nullable
  default VectorCursor asVectorCursor()
  {
    throw new UOE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor' on %s.", this.getClass().getName());
  }

  /**
   * Returns true if this {@link CursorHolder} supports calling {@link #asVectorCursor()}.
   */
  default boolean canVectorize()
  {
    return false;
  }

  /**
   * Release any resources acquired by cursors.
   */
  @Override
  default void close()
  {
    // nothing to close
  }

  CursorHolder EMPTY = new CursorHolder()
  {
    @Override
    public boolean canVectorize()
    {
      return true;
    }

    @Nullable
    @Override
    public Cursor asCursor()
    {
      return null;
    }

    @Nullable
    @Override
    public VectorCursor asVectorCursor()
    {
      return null;
    }
  };
}
