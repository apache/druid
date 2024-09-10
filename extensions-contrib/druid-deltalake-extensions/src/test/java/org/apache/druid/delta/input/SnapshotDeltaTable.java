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
import java.util.List;
import java.util.Map;

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
  public static final String DELTA_TABLE_PATH = "src/test/resources/snapshot-table";

  /**
   * The list of dimensions in the Delta table {@link #DELTA_TABLE_PATH}.
   */
  public static final List<String> DIMENSIONS = ImmutableList.of("id", "map_info");

  public static final List<Map<String, Object>> V0_SNAPSHOT_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "id", 0L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 1L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 2L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          )
      )
  );

  public static final List<Map<String, Object>> V1_SNAPSHOT_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "id", 0L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          ),
          ImmutableMap.of(
              "id", 2L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          )
      )
  );

  public static final List<Map<String, Object>> V2_SNAPSHOT_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "id", 2L,
              "map_info", ImmutableMap.of("snapshotVersion", 2)
          ),
          ImmutableMap.of(
              "id", 0L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          )
      )
  );

  public static final List<Map<String, Object>> LATEST_SNAPSHOT_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "id", 1L,
              "map_info", ImmutableMap.of("snapshotVersion", 3)
          ),
          ImmutableMap.of(
              "id", 4L,
              "map_info", ImmutableMap.of("snapshotVersion", 3)
          ),
          ImmutableMap.of(
              "id", 2L,
              "map_info", ImmutableMap.of("snapshotVersion", 2)
          ),
          ImmutableMap.of(
              "id", 0L,
              "map_info", ImmutableMap.of("snapshotVersion", 0)
          )
      )
  );

  /**
   * The Druid schema used for ingestion of {@link #DELTA_TABLE_PATH}.
   */
  public static final InputRowSchema FULL_SCHEMA = new InputRowSchema(
      new TimestampSpec("na", "posix", DateTimes.of("2024-01-01")),
      new DimensionsSpec(
          ImmutableList.of(
              new AutoTypeColumnSchema("id", null),
              new AutoTypeColumnSchema("map_info", null)
          )
      ),
      ColumnsFilter.all()
  );
}
