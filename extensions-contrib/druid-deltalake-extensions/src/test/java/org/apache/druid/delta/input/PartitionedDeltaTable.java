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
 * sample partitioned Delta Lake table used in the unit tests.
 *
 * <p>
 * For an unpartitioned delta table sample, see {@link NonPartitionedDeltaTable}.
 * </p>
 */
public class PartitionedDeltaTable
{
  /**
   * The Delta table path used by unit tests.
   */
  public static final String DELTA_TABLE_PATH = "src/test/resources/employee-delta-table-partitioned-name";

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
   * The expected set of rows from the first checkpoint file {@code {@link #DELTA_TABLE_PATH}/_delta_log/00000000000000000000.json}
   */
  private static final List<Map<String, Object>> SPLIT_0_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 898992000L,
              "name", "Employee1",
              "id", 1726247710L,
              "salary", 77928.75048595395,
              "age", (short) 25,
              "yoe", 3
          ),
          ImmutableMap.of(
              "birthday", 783475200L,
              "is_fulltime", true,
              "name", "Employee2",
              "id", 6142474489L,
              "salary", 57807.64358288189,
              "age", (short) 29,
              "yoe", 1
          ),
          ImmutableMap.of(
              "birthday", 989712000L,
              "name", "Employee3",
              "id", 3550221591L,
              "salary", 58226.41814823942,
              "age", (short) 22,
              "yoe", 6
          ),
          ImmutableMap.of(
              "birthday", 1130025600L,
              "name", "Employee4",
              "id", 3822742702L,
              "salary", 63581.29293955827,
              "age", (short) 18,
              "yoe", 2
          ),
          ImmutableMap.of(
              "birthday", 1001116800L,
              "name", "Employee5",
              "id", 5611620190L,
              "salary", 76076.68269796186,
              "age", (short) 22,
              "yoe", 3
          )
      )
  );

  /**
   * The expected rows from second checkpoint file {@code DELTA_TABLE_PATH/_delta_log/00000000000000000001.json}
   */
  private static final List<Map<String, Object>> SPLIT_1_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 1058227200L,
              "is_fulltime", false,
              "name", "Employee1",
              "id", 74065452L,
              "salary", 73109.56096784897,
              "age", (short) 20,
              "yoe", 3
          ),
          ImmutableMap.of(
              "birthday", 930528000L,
              "is_fulltime", true,
              "name", "Employee2",
              "id", 7246574606L,
              "salary", 54723.608212239684,
              "age", (short) 24,
              "yoe", 5
          ),
          ImmutableMap.of(
              "birthday", 863654400L,
              "is_fulltime", true,
              "bonus", 1424.9856f,
              "name", "Employee3",
              "id", 743868531L,
              "salary", 59595.17550553535,
              "last_vacation_time", 1712918081000L,
              "age", (short) 26,
              "yoe", 8
          ),
          ImmutableMap.of(
              "birthday", 850780800L,
              "name", "Employee4",
              "id", 4750981713L,
              "salary", 85673.13564089558,
              "age", (short) 27,
              "yoe", 8
          ),
          ImmutableMap.of(
              "birthday", 986256000L,
              "name", "Employee5",
              "id", 2605140287L,
              "salary", 56740.37076828715,
              "age", (short) 23,
              "yoe", 5
          )
      )
  );

  /**
   * The expected rows from second checkpoint file {@code DELTA_TABLE_PATH/_delta_log/00000000000000000001.json}
   */
  private static final List<Map<String, Object>> SPLIT_2_EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 885168000L,
              "name", "Employee1",
              "id", 4922151803L,
              "salary", 63418.10754490299,
              "age", (short) 26,
              "yoe", 10
          ),
          ImmutableMap.of(
              "birthday", 806198400L,
              "name", "Employee2",
              "id", 9345771736L,
              "salary", 58610.730719740226,
              "age", (short) 28,
              "yoe", 10
          ),
          ImmutableMap.of(
              "birthday", 1120435200L,
              "name", "Employee3",
              "id", 4740025087L,
              "salary", 63256.1008903906,
              "age", (short) 18,
              "yoe", 1
          ),
          ImmutableMap.of(
              "birthday", 968284800L,
              "is_fulltime", false,
              "name", "Employee4",
              "id", 655456941L,
              "salary", 95552.47057273184,
              "age", (short) 23,
              "yoe", 1
          ),
          ImmutableMap.of(
              "birthday", 1124841600L,
              "name", "Employee5",
              "id", 5565370685L,
              "salary", 74066.92920109774,
              "age", (short) 18,
              "yoe", 1
          )
      )
  );

  /**
   * Mapping of checkpoint file identifier to the list of expected rows in that checkpoint.
   */
  public static final Map<Integer, List<Map<String, Object>>> SPLIT_TO_EXPECTED_ROWS = new HashMap<>(
      ImmutableMap.of(
          0, SPLIT_0_EXPECTED_ROWS,
          1, SPLIT_1_EXPECTED_ROWS,
          2, SPLIT_2_EXPECTED_ROWS
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
}
