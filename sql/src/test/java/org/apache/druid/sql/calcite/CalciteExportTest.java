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

package org.apache.druid.sql.calcite;

import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.junit.Test;

public class CalciteExportTest extends CalciteIngestionDmlTest
{
  @Test
  public void testReplaceIntoExtern()
  {
    testIngestionQuery()
        .sql("REPLACE INTO EXTERN(s3(uri=\"s3://druid-data/exportdest/\",username=\"user1\")) AS CSV OVERWRITE ALL SELECT dim2 FROM foo PARTITIONED BY ALL")
        .expectQuery(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      "foo"
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        )
        .expectResources(dataSourceRead("foo"))
        .expectTarget("extern", RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }
}
