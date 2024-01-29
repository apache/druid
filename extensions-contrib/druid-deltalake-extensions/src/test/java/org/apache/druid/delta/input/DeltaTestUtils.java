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
import com.google.common.collect.ImmutableSet;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.types.StructType;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Refer to extensions-contrib/druid-deltalake-extensions/src/test/resources/README.md to generate the
 * sample Delta Lake table used in the unit tests.
 */
public class DeltaTestUtils
{
  /**
   * The Delta table path used by unit tests.
   */
  public static final String DELTA_TABLE_PATH = "src/test/resources/employee-delta-table";
  /**
   * The list of dimensions in the Delta table {@link #DELTA_TABLE_PATH}.
   */
  public static final List<String> DIMENSIONS = ImmutableList.of(
      "id",
      "birthday",
      "name",
      "age",
      "salary",
      "bonus",
      "yoe",
      "is_fulltime",
      "last_vacation_time"
  );

  /**
   * The expected set of rows from the first checkpoint file {@code DELTA_TABLE_PATH/_delta_log/00000000000000000000.json}
   */
  private static final List<Map<String, Object>> SPLIT_0_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 1057881600L,
              "name", "Employee1",
              "id", 867799346L,
              "salary", 87642.55209817083,
              "age", (short) 20,
              "yoe", 4
          ),
          ImmutableMap.of(
              "birthday", 1035417600L,
              "is_fulltime", false,
              "name", "Employee2",
              "id", 9963151889L,
              "salary", 79404.63969727767,
              "age", (short) 21,
              "yoe", 2
          ),
          ImmutableMap.of(
              "birthday", 890179200L,
              "name", "Employee3",
              "id", 2766777393L,
              "salary", 92418.21424435009,
              "age", (short) 25,
              "yoe", 9
          ),
          ImmutableMap.of(
              "birthday", 1073001600L,
              "name", "Employee4",
              "id", 6320361986L,
              "salary", 97907.76612488469,
              "age", (short) 20,
              "yoe", 3
          ),
          ImmutableMap.of(
              "birthday", 823996800L,
              "is_fulltime", true,
              "bonus", 4982.215f,
              "name", "Employee5",
              "id", 7068152260L,
              "salary", 79037.77202099308,
              "last_vacation_time", 1706256972000L,
              "age", (short) 27,
              "yoe", 9
          )
      )
  );

  /**
   * The expected rows from second checkpoint file {@code DELTA_TABLE_PATH/_delta_log/00000000000000000001.json}
   */
  private static final List<Map<String, Object>> SPLIT_1_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 937526400L,
              "is_fulltime", false,
              "name", "Employee1",
              "id", 4693651733L,
              "salary", 83845.11357786917,
              "age", (short) 24,
              "yoe", 3
          ),
          ImmutableMap.of(
              "birthday", 810777600L,
              "is_fulltime", false,
              "name", "Employee2",
              "id", 7132772589L,
              "salary", 90140.44051385639,
              "age", (short) 28,
              "yoe", 8
          ),
          ImmutableMap.of(
              "birthday", 1104969600L,
              "is_fulltime", true,
              "bonus", 3699.0881f,
              "name", "Employee3",
              "id", 6627278510L,
              "salary", 58857.27649436368,
              "last_vacation_time", 1706458554000L,
              "age", (short) 19,
              "yoe", 4
          ),
          ImmutableMap.of(
              "birthday", 763257600L,
              "is_fulltime", true,
              "bonus", 2334.6675f,
              "name", "Employee4",
              "id", 4786204912L,
              "salary", 93646.81222022788,
              "last_vacation_time", 1706390154000L,
              "age", (short) 29,
              "yoe", 5
          ),
          ImmutableMap.of(
              "birthday", 1114646400L,
              "name", "Employee5",
              "id", 2773939764L,
              "salary", 66300.05339373322,
              "age", (short) 18,
              "yoe", 3
          ),
          ImmutableMap.of(
              "birthday", 913334400L,
              "is_fulltime", false,
              "name", "Employee6",
              "id", 8333438088L,
              "salary", 59219.5257906128,
              "age", (short) 25,
              "yoe", 4
          ),
          ImmutableMap.of(
              "birthday", 893894400L,
              "is_fulltime", false,
              "name", "Employee7",
              "id", 8397454007L,
              "salary", 61909.733851830584,
              "age", (short) 25,
              "yoe", 8
          ),
          ImmutableMap.of(
              "birthday", 1038873600L,
              "is_fulltime", true,
              "bonus", 3000.0154f,
              "name", "Employee8",
              "id", 8925359945L,
              "salary", 76588.05471316943,
              "last_vacation_time", 1706195754000L,
              "age", (short) 21,
              "yoe", 1
          ),
          ImmutableMap.of(
              "birthday", 989798400L,
              "is_fulltime", true,
              "bonus", 4463.3833f,
              "name", "Employee9",
              "id", 8154788551L,
              "salary", 59787.98539015684,
              "last_vacation_time", 1706181354000L,
              "age", (short) 22,
              "yoe", 4
          ),
          ImmutableMap.of(
              "birthday", 912297600L,
              "is_fulltime", false,
              "name", "Employee10",
              "id", 5884382356L,
              "salary", 51565.91965119349,
              "age", (short) 25,
              "yoe", 9
          )
      )
  );

  /**
   * Mapping of checkpoint file identifier to the list of expected rows in that checkpoint.
   */
  public static final Map<Integer, List<Map<String, Object>>> SPLIT_TO_EXPECTED_ROWS = new HashMap<>(
      ImmutableMap.of(
          0, SPLIT_0_EXPECTED_ROWS,
          1, SPLIT_1_EXPECTED_ROWS
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
      new TimestampSpec("birthday", "posix", null),
      new DimensionsSpec(
          ImmutableList.of(
              new LongDimensionSchema("id"),
              new LongDimensionSchema("birthday"),
              new StringDimensionSchema("name"),
              new LongDimensionSchema("age"),
              new DoubleDimensionSchema("salary"),
              new FloatDimensionSchema("bonus"),
              new LongDimensionSchema("yoe"),
              new StringDimensionSchema("is_fulltime"),
              new LongDimensionSchema("last_vacation_time")
          )
      ),
      ColumnsFilter.all()
  );

  /**
   * Similar to {@link #FULL_SCHEMA}, but with a smaller set of columns with an inclusion filter applied.
   */
  public static final InputRowSchema SCHEMA_1 = new InputRowSchema(
      new TimestampSpec("birthday", "posix", null),
      new DimensionsSpec(
          ImmutableList.of(
              new LongDimensionSchema("id"),
              new LongDimensionSchema("birthday"),
              new StringDimensionSchema("name"),
              new LongDimensionSchema("age"),
              new DoubleDimensionSchema("salary"),
              new FloatDimensionSchema("bonus"),
              new LongDimensionSchema("yoe"),
              new StringDimensionSchema("is_fulltime"),
              new LongDimensionSchema("last_vacation_time")
          )
      ),
      ColumnsFilter.inclusionBased(ImmutableSet.of("id", "birthday", "name", "is_fulltime"))
  );

  /**
   * Similar to {@link #FULL_SCHEMA}, but with a smaller set of columns with an exclusion filter applied. A non-existent
   * column is added to the exclusion filter - it should silently get thrown away.
   */
  public static final InputRowSchema SCHEMA_2 = new InputRowSchema(
      new TimestampSpec("birthday", "posix", null),
      new DimensionsSpec(
          ImmutableList.of(
              new LongDimensionSchema("id"),
              new LongDimensionSchema("birthday"),
              new StringDimensionSchema("name"),
              new LongDimensionSchema("age"),
              new DoubleDimensionSchema("salary"),
              new FloatDimensionSchema("bonus"),
              new LongDimensionSchema("yoe"),
              new StringDimensionSchema("is_fulltime"),
              new LongDimensionSchema("last_vacation_time")
          )
      ),
      ColumnsFilter.exclusionBased(ImmutableSet.of("last_vacation_time", "bonus", "non_existent_column"))
  );

  /**
   * A simple wrapper that builds the table scan for {@link #DELTA_TABLE_PATH} meant to be used in tests.
   */
  public static Scan getScan(final TableClient tableClient) throws TableNotFoundException
  {
    final Table table = Table.forPath(tableClient, DELTA_TABLE_PATH);
    final Snapshot snapshot = table.getLatestSnapshot(tableClient);
    final StructType readSchema = snapshot.getSchema(tableClient);
    final ScanBuilder scanBuilder = snapshot.getScanBuilder(tableClient)
                                            .withReadSchema(tableClient, readSchema);
    return scanBuilder.build();
  }
}
