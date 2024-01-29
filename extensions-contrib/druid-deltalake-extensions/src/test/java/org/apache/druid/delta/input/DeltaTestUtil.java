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
import java.util.List;
import java.util.Map;

/**
 * Refer to extensions-contrib/druid-deltalake-extensions/src/test/resources/README.md to generate the
 * sample Delta Table used in the unit tests.
 */
public class DeltaTestUtil
{
  public static final String DELTA_TABLE_PATH = "src/test/resources/employee-delta-table";
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
  public static final List<Map<String, Object>> EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 944265600L,
              "id", 4662990160L,
              "name", "Employee1",
              "age", (short) 24,
              "salary", 63807.010056944906,
              "yoe", 5
          ),
          ImmutableMap.of(
              "id", 7436924672L,
              "birthday", 909878400L,
              "name", "Employee2",
              "age", (short) 25,
              "salary", 86178.47568217944,
              "yoe", 10,
              "is_fulltime", false
          ),
          ImmutableMap.of(
              "id", 1127302418L,
              "birthday", 901238400L,
              "name", "Employee3",
              "age", (short) 25,
              "salary", 59001.92470779706,
              "yoe", 10,
              "is_fulltime", false
          ),
          ImmutableMap.of(
              "id", 1810758014L,
              "birthday", 789177600L,
              "name", "Employee4",
              "age", (short) 29,
              "salary", 97151.7200456219,
              "bonus", 2880.2966f,
              "yoe", 10,
              "is_fulltime", true,
              "last_vacation_time", 1706309461000L
          ),
          ImmutableMap.of(
              "id", 2675583494L,
              "birthday", 950400000L,
              "name", "Employee5",
              "age", (short) 23,
              "salary", 84092.96929134917,
              "yoe", 6,
              "is_fulltime", false
          ),
          ImmutableMap.of(
              "id", 8109925563L,
              "birthday", 1030320000L,
              "name", "Employee1",
              "age", (short) 21,
              "salary", 98126.11562963494,
              "yoe", 6
          ),
          ImmutableMap.of(
              "id", 348540417L,
              "birthday", 1028764800L,
              "name", "Employee2",
              "salary", 88318.68501168216,
              "age", (short) 21,
              "yoe", 6
          ),
          ImmutableMap.of(
              "birthday", 772675200L,
              "is_fulltime", false,
              "name", "Employee3",
              "id", 644036573L,
              "salary", 70031.88789434545,
              "age", (short) 29,
              "yoe", 14
          ),
          ImmutableMap.of(
              "birthday", 940118400L,
              "name", "Employee4",
              "id", 8451974441L,
              "salary", 90127.23134932564,
              "age", (short) 24,
              "yoe", 4
          ),
          ImmutableMap.of(
              "birthday", 872294400L,
              "is_fulltime", false,
              "name", "Employee5",
              "id", 1257915386L,
              "salary", 55170.21435756755,
              "age", (short) 26,
              "yoe", 5
          ),
          ImmutableMap.of(
              "birthday", 1023148800L,
              "is_fulltime", false,
              "name", "Employee6",
              "id", 2034724452L,
              "salary", 97643.72021601905,
              "age", (short) 21,
              "yoe", 1
          ),
          ImmutableMap.of(
              "birthday", 1090627200L,
              "is_fulltime", true,
              "bonus", 3610.4019f,
              "name", "Employee7",
              "id", 1124457317L,
              "salary", 60433.78056730033,
              "last_vacation_time", 1706478632000L,
              "age", (short) 19,
              "yoe", 4
          ),
          ImmutableMap.of(
              "birthday", 867542400L,
              "is_fulltime", false,
              "name", "Employee8",
              "id", 8289790572L,
              "salary", 89266.9066406803,
              "age", (short) 26,
              "yoe", 1
          ),
          ImmutableMap.of(
              "birthday", 777945600L,
              "name", "Employee9",
              "id", 4197550591L,
              "salary", 82030.03829290869,
              "age", (short) 29,
              "yoe", 9
          ),
          ImmutableMap.of(
              "birthday", 1105747200L,
              "name", "Employee10",
              "id", 1628304468L,
              "salary", 87309.74810429095,
              "age", (short) 19,
              "yoe", 1
          )
      )
  );

  public static final InputRowSchema SCHEMA = new InputRowSchema(
      new TimestampSpec("birthday", "posix", null),
      new DimensionsSpec(ImmutableList.of(
          new LongDimensionSchema("id"),
          new LongDimensionSchema("birthday"),
          new StringDimensionSchema("name"),
          new LongDimensionSchema("age"),
          new DoubleDimensionSchema("salary"),
          new FloatDimensionSchema("bonus"),
          new LongDimensionSchema("yoe"),
          new StringDimensionSchema("is_fulltime"),
          new LongDimensionSchema("last_vacation_time")
      )),
      ColumnsFilter.all()
  );

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
