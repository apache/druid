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

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.types.StructType;

public class DeltaTestUtils
{
  public static Scan getScan(final TableClient tableClient, final String deltaTablePath) throws TableNotFoundException
  {
    final Table table = Table.forPath(tableClient, deltaTablePath);
    final Snapshot snapshot = table.getLatestSnapshot(tableClient);
    final StructType readSchema = snapshot.getSchema(tableClient);
    final ScanBuilder scanBuilder = snapshot.getScanBuilder(tableClient)
                                            .withReadSchema(tableClient, readSchema);
    return scanBuilder.build();
  }
}
