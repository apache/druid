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

package org.apache.druid.query.rowsandcols.concrete;

import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumnsTestBase;
import java.util.function.Function;

public class FrameRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public FrameRowsAndColumnsTest()
  {
    super(FrameRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, FrameRowsAndColumns> MAKER = input -> {

    return buildFrame(input);
  };

  public static FrameRowsAndColumns buildFrame(MapOfColumnsRowsAndColumns input)
  {
    LazilyDecoratedRowsAndColumns rac = new LazilyDecoratedRowsAndColumns(input, null, null, null, OffsetLimit.limit(Integer.MAX_VALUE), null, null);

    rac.numRows(); // materialize

    return (FrameRowsAndColumns) rac.getBase();
  }
}
