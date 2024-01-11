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
  public static final String DELTA_TABLE_PATH = "src/test/resources/people-delta-table";
  public static final List<String> DIMENSIONS = ImmutableList.of("city", "state", "surname", "email", "country");
  public static final List<Map<String, Object>> EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 1049418130358332L,
              "country", "Panama",
              "city", "Eastpointe",
              "surname", "Francis",
              "name", "Darren",
              "state", "Minnesota",
              "email", "rating1998@yandex.com"
          ),
          ImmutableMap.of(
              "birthday", 1283743763753323L,
              "country", "Aruba",
              "city", "Wheaton",
              "surname", "Berger",
              "name", "Madelene",
              "state", "New York",
              "email", "invitations2036@duck.com"
          ),
          ImmutableMap.of(
              "birthday", 1013053015543401L,
              "country", "Anguilla",
              "city", "Sahuarita",
              "surname", "Mccall",
              "name", "Anibal",
              "state", "Oklahoma",
              "email", "modifications2025@yahoo.com"
          ),
          ImmutableMap.of(
              "birthday", 569564422313618L,
              "country", "Luxembourg",
              "city", "Santa Rosa",
              "surname", "Jackson",
              "name", "Anibal",
              "state", "New Hampshire",
              "email", "medication1855@gmail.com"
          ),
          ImmutableMap.of(
              "birthday", 667560498632507L,
              "country", "Anguilla",
              "city", "Morristown",
              "surname", "Tanner",
              "name", "Loree",
              "state", "New Hampshire",
              "email", "transport1961@duck.com"
          ),
          ImmutableMap.of(
              "birthday", 826120534655077L,
              "country", "Panama",
              "city", "Greenville",
              "surname", "Gamble",
              "name", "Bernardo",
              "state", "North Carolina",
              "email", "limitations1886@yandex.com"
          ),
          ImmutableMap.of(
              "birthday", 1284652116668688L,
              "country", "China",
              "city", "Albert Lea",
              "surname", "Cherry",
              "name", "Philip",
              "state", "Nevada",
              "email", "const1874@outlook.com"
          ),
          ImmutableMap.of(
              "birthday", 1154549284242934L,
              "country", "Barbados",
              "city", "Mount Pleasant",
              "surname", "Beasley",
              "name", "Shaneka",
              "state", "Montana",
              "email", "msg1894@example.com"
          ),
          ImmutableMap.of(
              "birthday", 1034695930678172L,
              "country", "Honduras",
              "city", "Hutchinson",
              "surname", "Vinson",
              "name", "Keneth",
              "state", "Connecticut",
              "email", "questions2074@gmail.com"
          ),
          ImmutableMap.of(
              "birthday", 1166606855236945L,
              "country", "Senegal",
              "city", "Galt",
              "surname", "Schwartz",
              "name", "Hee",
              "state", "New Jersey",
              "email", "statements2016@protonmail.com"
          )
      )
  );

  public static final InputRowSchema SCHEMA = new InputRowSchema(
      new TimestampSpec("birthday", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS)),
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
