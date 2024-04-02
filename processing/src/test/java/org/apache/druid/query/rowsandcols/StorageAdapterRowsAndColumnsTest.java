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

import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumnsTest;
import org.apache.druid.segment.StorageAdapter;

import java.util.function.Function;

public class StorageAdapterRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public StorageAdapterRowsAndColumnsTest()
  {
    super(StorageAdapterRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, StorageAdapterRowsAndColumns> MAKER = input -> {
    return buildFrame(input);
  };

  private static StorageAdapterRowsAndColumns buildFrame(MapOfColumnsRowsAndColumns input)
  {
    FrameRowsAndColumns fRAC = FrameRowsAndColumnsTest.buildFrame(input);
    return new StorageAdapterRowsAndColumns(fRAC.as(StorageAdapter.class));
  }
}
