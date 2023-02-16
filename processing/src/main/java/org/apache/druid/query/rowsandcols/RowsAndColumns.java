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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.FramedOnHeapAggregatable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * An interface representing a chunk of RowsAndColumns.  Essentially a RowsAndColumns is just a batch of rows
 * with columns.
 * <p>
 * This interface has very little prescriptively defined about what *must* be implemented.  This is intentional
 * as there are lots of different possible representations of batch of rows each with their own unique positives
 * and negatives when it comes to processing.  So, any explicit definition of what a RowsAndColumns is will actually,
 * by definition, end up as optimal for one specific configuration and sub-optimal for others.  Instead of trying to
 * explicitly expand the interface to cover all the different possible ways that someone could want to interace
 * with a Rows and columns, we rely on semantic interfaces using the {@link RowsAndColumns#as} method instead.
 * <p>
 * That is, the expectation is that anything that works with a RowsAndColumns will tend to first ask the RowsAndColumns
 * object to become some other interface, for example, a {@link FramedOnHeapAggregatable}.  If a RowsAndColumns knows
 * how to do a good job as the requested interface, it can return its own concrete implementation of the interface and
 * run the necessary logic in its own optimized fashion.  If the RowsAndColumns instance does not know how to implement
 * the semantic interface, it is expected that a default implementation of the interface can be instantiated on top of
 * the default column access mechanisms that the RowsAndColumns provides.  Such default implementations should be
 * functionally correct, but are not believed to be optimal.
 * <p>
 * The "default column access mechanisms" here amount to using {@link #findColumn} to load a Column
 * and then using {@link Column#toAccessor} to access the individual cells of the column.  There is also a
 * {@link Column#as} method which a default implementation might attempt to use to create a more optimal runtime.
 * <p>
 * It is intended that this interface can be used by Frames, Segments and even normal on-heap JVM data structures to
 * participate in query operations.
 */
public interface RowsAndColumns
{
  @Nonnull
  static AppendableRowsAndColumns expectAppendable(RowsAndColumns input)
  {
    if (input instanceof AppendableRowsAndColumns) {
      return (AppendableRowsAndColumns) input;
    }

    AppendableRowsAndColumns retVal = input.as(AppendableRowsAndColumns.class);
    if (retVal == null) {
      retVal = new AppendableMapOfColumns(input);
    }
    return retVal;
  }

  /**
   * The set of column names available from the RowsAndColumns
   *
   * @return The set of column names available from the RowsAndColumns
   */
  @SuppressWarnings("unreachable")
  Collection<String> getColumnNames();

  /**
   * The number of rows in the RowsAndColumns object
   *
   * @return the integer number of rows
   */
  int numRows();

  /**
   * Finds a column by name.  null is returned if the column is not found.  The RowsAndColumns object should not
   * attempt to default not-found columns to pretend as if they exist, instead the user of the RowsAndColumns object
   * should decide the correct semantic interpretation of a column that does not exist.  It is expected that most
   * locations will choose to believe that the column does exist and is always null, but there are often optimizations
   * that can effect this same assumption without doing a lot of extra work if the calling code knows that it does not
   * exist.
   *
   * @param name the name of the column to find
   * @return the Column, if found.  null if not found.
   */
  @Nullable
  Column findColumn(String name);

  /**
   * Asks the RowsAndColumns to return itself as a concrete implementation of a specific interface.  The interface
   * asked for will tend to be a semantically-meaningful interface.  This method allows the calling code to interrogate
   * the RowsAndColumns object about whether it can offer a meaningful optimization of the semantic interface.  If a
   * RowsAndColumns cannot do anything specifically optimal for the interface requested, it should return null instead
   * of trying to come up with its own default implementation.
   *
   * @param clazz A class object representing the interface that the calling code wants a concrete implementation of
   * @param <T>   The interface that the calling code wants a concrete implementation of
   * @return A concrete implementation of the interface, or null if there is no meaningful optimization to be had
   * through a local implementation of the interface.
   */
  @Nullable
  <T> T as(Class<T> clazz);
}
