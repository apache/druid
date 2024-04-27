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

package org.apache.druid.testsEx.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.InputFormat;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(DruidTestRunner.class)
@Category(InputFormat.class)
public class ITLocalInputSourceAllFormatSchemalessTest extends AbstractLocalInputSourceParallelIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task_schemaless.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_schemaless_queries.json";

  @Test
  public void testAvroInputFormatIndexDataIngestionSpecWithFileSchemaSchemaless() throws Exception
  {
    List<Object> fieldList = ImmutableList.of(
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
    Map<String, Object> schema = ImmutableMap.of(
                                 "namespace", "org.apache.druid.data.input",
                                 "type", "record",
                                 "name", "wikipedia",
                                 "fields", fieldList);
    doIndexTest(
        AbstractITBatchIndexTest.InputFormatDetails.AVRO,
        INDEX_TASK,
        INDEX_QUERIES_RESOURCE,
        true,
        ImmutableMap.of("USE_NESTED_COLUMN_INDEXER", true),
        ImmutableMap.of("schema", schema),
        new Pair<>(false, false)
    );
  }

  @Test
  public void testAvroInputFormatIndexDataIngestionSpecNoFileSchemaSchemaless() throws Exception
  {
    doIndexTest(
        AbstractITBatchIndexTest.InputFormatDetails.AVRO,
        INDEX_TASK,
        INDEX_QUERIES_RESOURCE,
        true,
        ImmutableMap.of("USE_NESTED_COLUMN_INDEXER", true),
        Collections.emptyMap(),
        new Pair<>(false, false)
    );
  }

  @Test
  public void testJsonInputFormatIndexDataIngestionSpecSchemaless() throws Exception
  {
    doIndexTest(
        AbstractITBatchIndexTest.InputFormatDetails.JSON,
        INDEX_TASK,
        INDEX_QUERIES_RESOURCE,
        true,
        ImmutableMap.of("USE_NESTED_COLUMN_INDEXER", true),
        Collections.emptyMap(),
        new Pair<>(false, false)
    );
  }

  @Test
  public void testParquetInputFormatIndexDataIngestionSpecSchemaless() throws Exception
  {
    doIndexTest(
        AbstractITBatchIndexTest.InputFormatDetails.PARQUET,
        INDEX_TASK,
        INDEX_QUERIES_RESOURCE,
        true,
        ImmutableMap.of("USE_NESTED_COLUMN_INDEXER", true),
        Collections.emptyMap(),
        new Pair<>(false, false)
    );
  }

  @Test
  public void testOrcInputFormatIndexDataIngestionSpecSchemaless() throws Exception
  {
    doIndexTest(
        AbstractITBatchIndexTest.InputFormatDetails.ORC,
        INDEX_TASK,
        INDEX_QUERIES_RESOURCE,
        true,
        ImmutableMap.of("USE_NESTED_COLUMN_INDEXER", true),
        Collections.emptyMap(),
        new Pair<>(false, false)
    );
  }
}
