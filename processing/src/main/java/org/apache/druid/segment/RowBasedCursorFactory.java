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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.SimpleSequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.List;

public class RowBasedCursorFactory<RowType> implements CursorFactory
{
  private final Sequence<RowType> rowSequence;
  private final RowAdapter<RowType> rowAdapter;
  private final RowSignature rowSignature;

  public RowBasedCursorFactory(
      Sequence<RowType> rowSequence,
      RowAdapter<RowType> rowAdapter,
      RowSignature rowSignature
  )
  {
    this.rowSequence = rowSequence;
    this.rowAdapter = rowAdapter;
    this.rowSignature = rowSignature;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    // It's in principle incorrect for sort order to be __time based here, but for historical reasons, we're keeping
    // this in place for now. The handling of "interval" in "RowBasedCursor", which has been in place for some time,
    // suggests we think the data is always sorted by time.
    final List<OrderBy> ordering;
    final boolean descending;
    if (Cursors.preferDescendingTimeOrdering(spec)) {
      ordering = Cursors.descendingTimeOrder();
      descending = true;
    } else {
      ordering = Cursors.ascendingTimeOrder();
      descending = false;
    }
    return new CursorHolder()
    {
      final Closer closer = Closer.create();

      @Override
      public Cursor asCursor()
      {
        final RowWalker<RowType> rowWalker = closer.register(
            new RowWalker<>(descending ? reverse(rowSequence) : rowSequence, rowAdapter)
        );
        return new RowBasedCursor<>(
            rowWalker,
            rowAdapter,
            spec.getFilter(),
            spec.getInterval(),
            spec.getVirtualColumns(),
            descending,
            rowSignature
        );
      }

      @Nullable
      @Override
      public List<OrderBy> getOrdering()
      {
        return ordering;
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
  }

  @Override
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return RowBasedColumnSelectorFactory.getColumnCapabilities(rowSignature, column);
  }

  /**
   * Reverse a Sequence.
   *
   * If the Sequence is a {@link SimpleSequence}, this avoids materialization because its
   * {@link SimpleSequence#toList()} method returns a view of the underlying list. Otherwise, the list will be
   * materialized and then reversed.
   */
  private static <T> Sequence<T> reverse(final Sequence<T> sequence)
  {
    if (sequence instanceof SimpleSequence) {
      // Extract the Iterable from the SimpleSequence, so we can reverse it without copying if it is List-backed.
      return Sequences.simple(reverse(((SimpleSequence<T>) sequence).getIterable()));
    } else {
      // Materialize and reverse the objects.
      return Sequences.simple(Lists.reverse(sequence.toList()));
    }
  }

  /**
   * Reverse an Iterable. Will avoid materialization if possible, but, this is not always possible.
   */
  private static <T> Iterable<T> reverse(final Iterable<T> iterable)
  {
    if (iterable instanceof List) {
      return Lists.reverse((List<T>) iterable);
    } else {
      // Materialize and reverse the objects. Note that this means reversing non-List Iterables will use extra memory.
      return Lists.reverse(Lists.newArrayList(iterable));
    }
  }
}
