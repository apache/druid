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

import java.util.ArrayList;
import java.util.function.Function;

public class ConcatRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public ConcatRowsAndColumnsTest()
  {
    super(ConcatRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, ConcatRowsAndColumns> MAKER = input -> {
    int rowsPerChunk = Math.max(1, input.numRows() / 4);

    ArrayList<RowsAndColumns> theRac = new ArrayList<>();

    int startId = 0;
    while (startId < input.numRows()) {
      theRac.add(new LimitedRowsAndColumns(input, startId, Math.min(input.numRows(), startId + rowsPerChunk)));
      startId += rowsPerChunk;
    }

    return new ConcatRowsAndColumns(theRac);
  };
}
