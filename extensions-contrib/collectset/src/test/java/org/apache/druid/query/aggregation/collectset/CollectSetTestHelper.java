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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CollectSetTestHelper
{
  public static final String DATETIME = "t";
  public static final String[] DIMENSIONS = {"dim1", "dim2", "dim3", "dim4"};

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(DATETIME, "iso", DateTimes.of("2000")),
          new DimensionsSpec(
              ImmutableList.of(
                  new StringDimensionSchema("dim1"),
                  new StringDimensionSchema("dim2"),
                  new StringDimensionSchema("dim3"),
                  new StringDimensionSchema("dim4")
              ),
              null,
              null
          )
      )
  );

  public static final List<InputRow> INPUT_ROWS = ImmutableList.<Map<String, Object>>of(
      ImmutableMap.of(DIMENSIONS[0], "0", DIMENSIONS[1], "iphone", DIMENSIONS[2], "video", DIMENSIONS[3], ImmutableList.of("tag1", "tag2", "tag3")),
      ImmutableMap.of(DIMENSIONS[0], "1", DIMENSIONS[1], "iphone", DIMENSIONS[2], "video", DIMENSIONS[3], ""),
      ImmutableMap.of(DIMENSIONS[0], "0", DIMENSIONS[1], "android", DIMENSIONS[2], "image", DIMENSIONS[3], ImmutableList.of("tag1", "tag4", "tag5", "tag6")),
      ImmutableMap.of(DIMENSIONS[0], "2", DIMENSIONS[1], "android", DIMENSIONS[2], "video", DIMENSIONS[3], "tag2"),
      ImmutableMap.of(DIMENSIONS[0], "0", DIMENSIONS[1], "iphone", DIMENSIONS[2], "text", DIMENSIONS[3], ImmutableList.of("tag4", "tag5", "tag7", "tag8"))
  ).stream().map(e -> PARSER.parseBatch(e).get(0)).collect(Collectors.toList());

}
