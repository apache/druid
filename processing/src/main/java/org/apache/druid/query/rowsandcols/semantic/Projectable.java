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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.List;

/**
 * A semantic interface used to project a RowsAndColumns.  This interface should be used when it is important to
 * materialize results from a RowsAndColumns instead of relying on the eventual application of lazy adjustments
 */
public interface Projectable
{
  /**
   * Materializes a projection, the returned RowsAndColumns object here should have an independent lifecycle
   * from the RowsAndColumns object(s) it is based on.
   *
   * @param columns the columns to project
   * @return a fully materialized RowsAndColumns object that has an independent lifecycle from its base
   */
  RowsAndColumns project(List<String> columns);
}
