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

import org.apache.druid.error.DruidException;
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
        .sql("REPLACE INTO EXTERN(s3(bucket=\"bucket1\",prefix=\"prefix1\",tempDir=\"/tempdir\",chunkSize=\"5242880\",maxRetry=\"1\")) "
             + "AS CSV "
             + "OVERWRITE ALL "
             + "SELECT dim2 FROM foo")
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
        .expectTarget("EXTERN", RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testExportWithPartitionedBy()
  {
    testIngestionQuery()
        .sql("REPLACE INTO EXTERN(s3(bucket=\"bucket1\",prefix=\"prefix1\",tempDir=\"/tempdir\",chunkSize=\"5242880\",maxRetry=\"1\")) "
             + "AS CSV "
             + "OVERWRITE ALL "
             + "SELECT dim2 FROM foo "
             + "PARTITIONED BY ALL")
        .expectValidationError(
            DruidException.class,
            "Export statements do not currently support a PARTITIONED BY or CLUSTERED BY clause."
        )
        .verify();
  }

  @Test
  public void testInsertIntoExtern()
  {
    testIngestionQuery()
        .sql("INSERT INTO EXTERN(s3(bucket=\"bucket1\",prefix=\"prefix1\",tempDir=\"/tempdir\",chunkSize=\"5242880\",maxRetry=\"1\")) "
             + "AS CSV "
             + "SELECT dim2 FROM foo")
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
        .expectTarget("EXTERN", RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testExportWithoutFormat()
  {
    testIngestionQuery()
        .sql("INSERT INTO EXTERN(s3(bucket=\"bucket1\",prefix=\"prefix1\",tempDir=\"/tempdir\",chunkSize=\"5242880\",maxRetry=\"1\")) "
             + "SELECT dim2 FROM foo")
        .expectValidationError(
            DruidException.class,
            "Exporting rows into an EXTERN destination requires an AS clause to specify the format, but none was found."
        )
        .verify();
  }

  @Test
  public void testSelectFromTableNamedExport()
  {
    testIngestionQuery()
        .sql("INSERT INTO csv SELECT dim2 FROM foo PARTITIONED BY ALL")
        .expectQuery(
            Druids.newScanQueryBuilder()
                  .dataSource("foo")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        )
        .expectResources(dataSourceRead("foo"), dataSourceWrite("csv"))
        .expectTarget("csv", RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testNormalInsertWithFormat()
  {
    testIngestionQuery()
        .sql("REPLACE INTO testTable "
             + "AS CSV "
             + "OVERWRITE ALL "
             + "SELECT dim2 FROM foo "
             + "PARTITIONED BY ALL")
        .expectValidationError(
            DruidException.class,
            "The AS <format> clause should only be specified while exporting rows into an EXTERN destination."
        )
        .verify();
  }
}
