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

package org.apache.druid.segment.data;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

/**
 * A single-entry {@link Indexed} of {@link ByteBuffer}, far lighter than a {@link GenericIndexed} for the degenerate
 * one-value case (e.g. a constant column's dictionary). {@link #get} returns a fresh {@link ByteBuffer#duplicate()}
 * each call so callers that advance the buffer's position while decoding (e.g. {@link
 * org.apache.druid.java.util.common.StringUtils#fromUtf8(ByteBuffer)}) never corrupt the shared value or race with
 * concurrent readers. The single value is treated as immutable: its position/limit are never mutated by this class.
 */
public final class ConstantUtf8Indexed implements Indexed<ByteBuffer>
{
  @Nullable
  private final ByteBuffer value;

  public ConstantUtf8Indexed(@Nullable ByteBuffer value)
  {
    this.value = value;
  }

  @Override
  public int size()
  {
    return 1;
  }

  @Nullable
  @Override
  public ByteBuffer get(int index)
  {
    if (index != 0) {
      throw DruidException.defensive("index[%s] out of bounds for a single-value dictionary", index);
    }
    return value == null ? null : value.duplicate();
  }

  @Override
  public int indexOf(@Nullable ByteBuffer other)
  {
    // utf8Comparator handles nulls (nulls first, matching the dictionary sort order) and reads via absolute indexing
    // without advancing positions, so the shared value can be passed directly.
    final int comparison = ByteBufferUtils.utf8Comparator().compare(value, other);
    if (comparison == 0) {
      return 0;
    }
    // Single-entry sorted lookup: mirror GenericIndexed#indexOf's not-found encoding of -(insertionPoint) - 1.
    // value < other -> other would insert after it (point 1 -> -2); value > other -> before it (point 0 -> -1).
    return comparison < 0 ? -2 : -1;
  }

  @Override
  public Iterator<ByteBuffer> iterator()
  {
    return Collections.singletonList(value == null ? null : value.duplicate()).iterator();
  }

  @Override
  public boolean isSorted()
  {
    // A single-entry dictionary is trivially sorted (and unique).
    return true;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("value", value);
  }
}
