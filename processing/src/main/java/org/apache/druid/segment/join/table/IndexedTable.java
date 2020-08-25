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

package org.apache.druid.segment.join.table;

import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ReferenceCountedObject;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 * An interface to a table where some columns (the 'key columns') have indexes that enable fast lookups.
 *
 * The main user of this class is {@link IndexedTableJoinable}, and its main purpose is to participate in joins.
 */
public interface IndexedTable extends ReferenceCountedObject, Closeable
{
  /**
   * Returns the version of this table, used to compare against when loading a new version of the table
   */
  @SuppressWarnings("unused")
  String version();

  /**
   * Returns the columns of this table that have indexes.
   */
  Set<String> keyColumns();

  /**
   * Returns the signature of this table, which includes all key columns (as well as other columns that can be
   * selected, but are not keys).
   */
  RowSignature rowSignature();

  /**
   * Returns the number of rows in this table. It must not change over time, since it is used for things like algorithm
   * selection and reporting of cardinality metadata.
   */
  int numRows();

  /**
   * Returns the index for a particular column. The provided column number must be that column's position in
   * {@link #rowSignature()}.
   */
  Index columnIndex(int column);

  /**
   * Returns a reader for a particular column. The provided column number must be that column's position in
   * {@link #rowSignature()}. Don't forget to close your {@link Reader} when finished reading, to clean up any
   * resources.
   */
  Reader columnReader(int column);

  /**
   * This method allows a table to directly provide an optimized {@link ColumnSelectorFactory} for
   * {@link IndexedTableJoinMatcher} to create selectors. If this method returns null, the default
   * {@link IndexedTableColumnSelectorFactory}, which creates {@link IndexedTableDimensionSelector} or
   * {@link IndexedTableColumnValueSelector} as appropriate, both backed with a {@link #columnReader}, will be used
   * instead.
   */
  @Nullable
  default ColumnSelectorFactory makeColumnSelectorFactory(ReadableOffset offset, boolean descending, Closer closer)
  {
    return null;
  }

  /**
   * Indexes support fast lookups on key columns.
   */
  interface Index
  {
    /**
     * Returns the list of row numbers where the column this Reader is based on contains 'key'.
     */
    IntList find(Object key);
  }

  /**
   * Readers support reading values out of any column.
   */
  interface Reader extends Closeable
  {
    /**
     * Read the value at a particular row number. Throws an exception if the row is out of bounds (must be between zero
     * and {@link #numRows()}).
     */
    @Nullable
    Object read(int row);

    @Override
    default void close() throws IOException
    {
      // nothing to close
    }
  }
}
