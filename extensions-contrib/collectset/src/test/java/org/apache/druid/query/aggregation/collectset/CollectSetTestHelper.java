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

package org.apache.druid.query.aggregation.collectset;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectSetTestHelper
{
  public static final String DATETIME = "t";
  public static final String[] DIMENSIONS = {"dim1", "dim2", "dim3"};
  public static final String[][] ROWS =
  {
      {"2000-01-01T00:00:00.000Z", "0", "iphone", "video"},
      {"2000-01-01T00:00:00.001Z", "1", "iphone", "video"},
      {"2000-01-01T00:00:00.002Z", "0", "android", "image"},
      {"2000-01-01T00:00:00.003Z", "2", "android", "video"},
      {"2000-01-01T00:00:00.004Z", "0", "iphone", "text"}
  };

  static final List<InputRow> INPUT_ROWS = new ArrayList<>();

  static {
    for (String[] row : ROWS) {
      INPUT_ROWS.add(
          new MapBasedInputRow(
              DateTimes.of(row[0]),
              Arrays.asList(DIMENSIONS),
              ImmutableMap.of(
                  DIMENSIONS[0], row[1],
                  DIMENSIONS[1], row[2],
                  DIMENSIONS[2], row[3]
              )
          )
      );
    }
  }
}
