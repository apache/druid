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

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class RowSerdeTest
{
  private static final String DELTA_TABLE_PATH = "src/test/resources/people-delta-table";

  @Test
  public void testSerializeDeserializeRoundtrip() throws TableNotFoundException
  {
    TableClient tableClient = DefaultTableClient.create(new Configuration());
    Table table = Table.forPath(tableClient, DELTA_TABLE_PATH);
    Snapshot snapshot = table.getLatestSnapshot(tableClient);
    StructType readSchema = snapshot.getSchema(tableClient);
    ScanBuilder scanBuilder = snapshot.getScanBuilder(tableClient)
                                      .withReadSchema(tableClient, readSchema);
    Row scanState = scanBuilder.build().getScanState(tableClient);

    String rowJson = RowSerde.serializeRowToJson(scanState);
    Row row = RowSerde.deserializeRowFromJson(tableClient, rowJson);

    Assert.assertEquals(scanState.getSchema(), row.getSchema());
  }


}