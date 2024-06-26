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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.vector.VectorCursor;

import javax.annotation.Nullable;

public interface CursorMaker
{
  /**
   * Create a {@link Sequence} of {@link Cursor} for use with non-vectorized query engines. Each {@link Cursor} of the
   * sequence corresponds to a {@link org.apache.druid.java.util.common.granularity.Granularity} bucket determined by
   * {@link CursorBuildSpec#getGranularity()}.
   * <p>
   * Consuming this {@link Sequence} will automatically close all resources associated with this {@link CursorMaker}
   * so calling {@link #cleanup()} is not needed.
   */
  Sequence<Cursor> makeCursors();

  /**
   * Create a {@link VectorCursor} for use with vectorized query engines.
   * <p>
   * Advancing this {@link VectorCursor} to the end or explicitly calling {@link VectorCursor#close()} will
   * automatically close all resources associated with this {@link CursorMaker} so calling {@link #cleanup()} is not
   * needed.
   */
  @Nullable
  default VectorCursor makeVectorCursor()
  {
    throw new UOE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor' on %s.", this.getClass().getName());
  }

  /**
   * Returns true if this {@link CursorMaker} supports creating vectorized selectors. This operation may acquire
   * underlying resources, so calling {@link #cleanup()} is necessary if no cursors are created and consumed.
   */
  default boolean canVectorize()
  {
    return false;
  }

  /**
   * Release any resources acquired if cursors are not consumed. Typically consuming a cursor or vector cursor releases
   * the resources upon completion, but if for some reason this will not happen, this method must be called.
   */
  default void cleanup()
  {
    // nothing to cleanup
  }

  CursorMaker EMPTY = new CursorMaker()
  {
    @Override
    public boolean canVectorize()
    {
      return true;
    }

    @Override
    public Sequence<Cursor> makeCursors()
    {
      return Sequences.empty();
    }

    @Nullable
    @Override
    public VectorCursor makeVectorCursor()
    {
      return null;
    }
  };
}
