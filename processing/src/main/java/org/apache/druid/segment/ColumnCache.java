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

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;

import javax.annotation.Nullable;
import java.util.HashMap;

public class ColumnCache implements ColumnSelector
{
  private final HashMap<String, ColumnHolder> holderCache;
  private final QueryableIndex index;
  private final Closer closer;

  public ColumnCache(QueryableIndex index, Closer closer)
  {
    this.index = index;
    this.closer = closer;

    this.holderCache = new HashMap<>();
  }


  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    return holderCache.computeIfAbsent(columnName, dimension -> {
      // Here we do a funny little dance to memoize the BaseColumn and register it with the closer.
      // It would probably be cleaner if the ColumnHolder itself was `Closeable` and did its own memoization,
      // but that change is much wider and runs the risk of even more things that need to close the thing
      // not actually closing it.  So, maybe this is a hack, maybe it's a wise decision, who knows, but at
      // least for now, we grab the holder, grab the column, register the column with the closer and then return
      // a new holder that always returns the same reference for the column.

      final ColumnHolder holder = index.getColumnHolder(columnName);
      if (holder == null) {
        return null;
      }

      return new ColumnHolder()
      {
        @Nullable
        private BaseColumn theColumn = null;

        @Override
        public ColumnCapabilities getCapabilities()
        {
          return holder.getCapabilities();
        }

        @Override
        public int getLength()
        {
          return holder.getLength();
        }

        @Override
        public BaseColumn getColumn()
        {
          if (theColumn == null) {
            theColumn = closer.register(holder.getColumn());
          }
          return theColumn;
        }

        @Nullable
        @Override
        public ColumnIndexSupplier getIndexSupplier()
        {
          return holder.getIndexSupplier();
        }

        @Override
        public SettableColumnValueSelector makeNewSettableColumnValueSelector()
        {
          return holder.makeNewSettableColumnValueSelector();
        }
      };
    });
  }

  @Nullable
  public BaseColumn getColumn(String columnName)
  {
    final ColumnHolder retVal = getColumnHolder(columnName);
    return retVal == null ? null : retVal.getColumn();
  }
}
