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

package org.apache.druid.query.rowsandcols.util;

public class FindResult
{
  public static FindResult found(int startRow, int endRow)
  {
    return new FindResult(startRow, endRow, true);
  }

  public static FindResult notFound(int nextRow)
  {
    return new FindResult(nextRow, -1, false);
  }

  private final int startRow;
  private final int endRow;
  private final boolean found;

  private FindResult(
      int startRow,
      int endRow,
      boolean found
  )
  {
    this.startRow = startRow;
    this.endRow = endRow;
    this.found = found;
  }

  public boolean wasFound()
  {
    return found;
  }

  public int getStartRow()
  {
    return startRow;
  }

  public int getEndRow()
  {
    return endRow;
  }

  public int getNext()
  {
    // We overload the startRow to represent the next meaningful row if the value wasn't actually found.
    return startRow;
  }
}
