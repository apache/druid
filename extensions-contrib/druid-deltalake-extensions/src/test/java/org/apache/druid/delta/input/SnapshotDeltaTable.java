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

package org.apache.druid.delta.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.AutoTypeColumnSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Refer to extensions-contrib/druid-deltalake-extensions/src/test/resources/README.md to generate the
 * sample complex types Delta Lake table used in the unit tests.
 *
 */
public class SnapshotDeltaTable
{
  /**
   * The Delta table path used by unit tests.
   */
//  public static final String DELTA_TABLE_PATH = "src/test/resources/snapshot-table";
  public static final String DELTA_TABLE_PATH = "src/test/resources/snapshot-table";

  /**
   * The list of dimensions in the Delta table {@link #DELTA_TABLE_PATH}.
   */
  public static final List<String> DIMENSIONS = ImmutableList.of(
      "id",
      "array_info",
      "struct_info",
      "map_info"
  );

  /**
   * The expected set of rows from the first checkpoint file {@code {@link #DELTA_TABLE_PATH}/_delta_log/00000000000000000000.json}
   */
  private static final List<Map<String, Object>> SPLIT_0_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "id", 0L,
              "array_info", ImmutableList.of(0, 1, 2, 3),
              "struct_info", ImmutableMap.of("id", 0L, "snapshotVersion", "0"),
              "map_info", ImmutableMap.of("key1", 0, "snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 1L,
              "array_info", ImmutableList.of(1, 2, 3, 4),
              "struct_info", ImmutableMap.of("id", 1L, "snapshotVersion", "0"),
              "map_info", ImmutableMap.of("key1", 1, "snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 2L,
              "array_info", ImmutableList.of(2, 3, 4, 5),
              "struct_info", ImmutableMap.of("id", 2L, "snapshotVersion", "0"),
              "map_info", ImmutableMap.of("key1", 2, "snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 3L,
              "array_info", ImmutableList.of(3, 4, 5, 6),
              "struct_info", ImmutableMap.of("id", 3L, "snapshotVersion", "0"),
              "map_info", ImmutableMap.of("key1", 3, "snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 4L,
              "array_info", ImmutableList.of(4, 5, 6, 7),
              "struct_info", ImmutableMap.of("id", 4L, "snapshotVersion", "0"),
              "map_info", ImmutableMap.of("key1", 4, "snapshotVersion", 0)
          )
      )
  );

  /**
   * Mapping of checkpoint file identifier to the list of expected rows in that checkpoint.
   */
  public static final Map<Integer, List<Map<String, Object>>> SPLIT_TO_EXPECTED_ROWS = new HashMap<>(
      ImmutableMap.of(
          0, SPLIT_0_EXPECTED_ROWS
      )
  );

  /**
   * Complete set of expected rows across all checkpoint files for {@link #DELTA_TABLE_PATH}.
   */
  public static final List<Map<String, Object>> EXPECTED_ROWS = SPLIT_TO_EXPECTED_ROWS.values().stream()
                                                                                      .flatMap(List::stream)
                                                                                      .collect(Collectors.toList());

  /**
   * The Druid schema used for ingestion of {@link #DELTA_TABLE_PATH}.
   */
  public static final InputRowSchema FULL_SCHEMA = new InputRowSchema(
      new TimestampSpec("na", "posix", DateTimes.of("2024-01-01")),
      new DimensionsSpec(
          ImmutableList.of(
              new AutoTypeColumnSchema("id", null),
              new AutoTypeColumnSchema("array_info", null),
              new AutoTypeColumnSchema("struct_info", null),
              new AutoTypeColumnSchema("map_info", null)
          )
      ),
      ColumnsFilter.all()
  );
}
