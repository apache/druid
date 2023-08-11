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

package org.apache.druid.query.rowsandcols.column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface representing a Column of data.
 *
 * This interface prescribes that a {@link ColumnAccessor} must be defined on the column, but also offers an
 * {@link #as} method to allow for optimized specific implementations of semantically meaningful logic.
 *
 * That is, the expectation is that some things work with Column objects might choose to first ask the Column
 * object to become some other interface.  If the Column knows how to do a good job as the requested interface, it can
 * return its own concrete implementation of the interface and run the necessary logic in its own optimized fashion.
 * If the Column instance does not know how to implement the semantic interface, it is expected that the
 * {@link ColumnAccessor} will be leveraged to implement whatever logic is required.
 */
public interface Column
{
  /**
   * Returns the column as a {@link ColumnAccessor}.  Semantically, this would be equivalent to calling
   * {@code Column.as(ColumnAccessor.class)}.  However, being able to implement this interface is part of the explicit
   * contract of implementing this interface, so instead of relying on {@link #as} which allows for returning null,
   * we define a top-level method that should never return null.
   *
   * @return a {@link ColumnAccessor} representation of the column, this should never return null.
   */
  @Nonnull
  ColumnAccessor toAccessor();

  /**
   * Asks the Column to return itself as a concrete implementation of a specific interface.  The interface
   * asked for will tend to be a semantically-meaningful interface.  This method allows the calling code to interrogate
   * the Column object about whether it can offer a meaningful optimization of the semantic interface.  If a
   * Column cannot do anything specifically optimal for the interface requested, it should return null instead
   * of trying to come up with its own default implementation.
   *
   * @param clazz A class object representing the interface that the calling code wants a concrete implementation of
   * @param <T> The interface that the calling code wants a concrete implementation of
   * @return A concrete implementation of the interface, or null if there is no meaningful optimization to be had
   * through a local implementation of the interface.
   */
  @Nullable
  <T> T as(Class<? extends T> clazz);
}
