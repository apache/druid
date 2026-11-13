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

import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * The rows of a system table that a caller may read, decided when the query is authorized. It is attached to a
 * {@link SystemTableDataSource} on the server that authorized the query and is never serialized, so a client can't
 * supply one.
 *
 * @param rowPredicate rows the caller may read, given in the table's row-signature order, or null if the caller may
 *                     read every row
 */
public record SystemTableAccess(@Nullable Predicate<Object[]> rowPredicate)
{
  /**
   * Access to every row of the table.
   */
  public static final SystemTableAccess ALL_ROWS = new SystemTableAccess(null);

  /**
   * Returns the rows the caller may read.
   */
  public Iterable<Object[]> filter(final Iterable<Object[]> rows)
  {
    return rowPredicate == null ? rows : Iterables.filter(rows, rowPredicate::test);
  }
}
