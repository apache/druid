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

package org.apache.druid.query;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedCursor;
import org.apache.druid.segment.RowWalker;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.function.Function;

public class IterableRowsCursorHelper
{
  public static RowBasedCursor<Object[]> getCursorFromIterable(Iterable<Object[]> rows, RowSignature rowSignature)
  {
    RowAdapter<Object[]> rowAdapter = new RowAdapter<Object[]>()
    {
      @Nonnull
      @Override
      public Function<Object[], Object> columnFunction(String columnName)
      {
        final int columnIndex = rowSignature.indexOf(columnName);
        if (columnIndex < 0) {
          return row -> null;
        }
        return row -> row[columnIndex];
      }
    };
    RowWalker<Object[]> rowWalker = new RowWalker<>(Sequences.simple(rows), rowAdapter);
    return new RowBasedCursor<>(
        rowWalker,
        rowAdapter,
        null,
        Intervals.ETERNITY, // Setting the interval to eternity ensures that we are iterating over all of the rows
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        rowSignature
    );
  }

  public static RowBasedCursor<Object[]> getCursorFromSequence(Sequence<Object[]> rows, RowSignature rowSignature)
  {
    return getCursorFromIterable(
        new Iterable<Object[]>()
        {
          Yielder<Object[]> yielder = Yielders.each(rows);

          @Override
          public Iterator<Object[]> iterator()
          {
            return new Iterator<Object[]>()
            {
              @Override
              public boolean hasNext()
              {
                return !yielder.isDone();
              }

              @Override
              public Object[] next()
              {
                Object[] retVal = yielder.get();
                yielder = yielder.next(null);
                return retVal;
              }
            };
          }
        },
        rowSignature
    );
  }
}
