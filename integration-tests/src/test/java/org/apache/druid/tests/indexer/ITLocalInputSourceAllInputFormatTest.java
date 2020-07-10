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

package org.apache.druid.tests.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

@Test(groups = TestNGGroup.INPUT_FORMAT)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITLocalInputSourceAllInputFormatTest extends AbstractLocalInputSourceParallelIndexTest
{
  @Test
  public void testAvroInputFormatIndexDataIngestionSpecWithSchema() throws Exception
  {
    List fieldList = ImmutableList.of(
        ImmutableMap.of("name", "timestamp", "type", "string"),
        ImmutableMap.of("name", "page", "type", "string"),
        ImmutableMap.of("name", "language", "type", "string"),
        ImmutableMap.of("name", "user", "type", "string"),
        ImmutableMap.of("name", "unpatrolled", "type", "string"),
        ImmutableMap.of("name", "newPage", "type", "string"),
        ImmutableMap.of("name", "robot", "type", "string"),
        ImmutableMap.of("name", "anonymous", "type", "string"),
        ImmutableMap.of("name", "namespace", "type", "string"),
        ImmutableMap.of("name", "continent", "type", "string"),
        ImmutableMap.of("name", "country", "type", "string"),
        ImmutableMap.of("name", "region", "type", "string"),
        ImmutableMap.of("name", "city", "type", "string"),
        ImmutableMap.of("name", "added", "type", "int"),
        ImmutableMap.of("name", "deleted", "type", "int"),
        ImmutableMap.of("name", "delta", "type", "int")
    );
    Map schema = ImmutableMap.of("namespace", "org.apache.druid.data.input",
                                 "type", "record",
                                 "name", "wikipedia",
                                 "fields", fieldList);
    doIndexTest(InputFormatDetails.AVRO, ImmutableMap.of("schema", schema));
  }

  @Test
  public void testAvroInputFormatIndexDataIngestionSpecWithoutSchema() throws Exception
  {
    doIndexTest(InputFormatDetails.AVRO);
  }

  @Test
  public void testJsonInputFormatIndexDataIngestionSpecWithSchema() throws Exception
  {
    doIndexTest(InputFormatDetails.JSON);
  }

  @Test
  public void testTsvInputFormatIndexDataIngestionSpecWithSchema() throws Exception
  {
    doIndexTest(InputFormatDetails.TSV, ImmutableMap.of("findColumnsFromHeader", true));
  }

  @Test
  public void testParquetInputFormatIndexDataIngestionSpecWithSchema() throws Exception
  {
    doIndexTest(InputFormatDetails.PARQUET);
  }

  @Test
  public void testOrcInputFormatIndexDataIngestionSpecWithSchema() throws Exception
  {
    doIndexTest(InputFormatDetails.ORC);
  }

  @Test
  public void testCsvInputFormatIndexDataIngestionSpecWithSchema() throws Exception
  {
    doIndexTest(InputFormatDetails.CSV, ImmutableMap.of("findColumnsFromHeader", true));
  }
}
