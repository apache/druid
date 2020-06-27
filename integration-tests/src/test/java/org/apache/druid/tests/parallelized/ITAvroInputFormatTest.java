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

package org.apache.druid.tests.parallelized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = TestNGGroup.BATCH_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAvroInputFormatTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  @Test
  public void testIndexDataIngestionSpecWithoutSchema() throws Exception
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
    Map arvoInputFormatMap = ImmutableMap.of("type", "avro_ocf", "schema", schema);
    doIndexTest(arvoInputFormatMap);
  }

  @Test
  public void testIndexDataIngestionSpecWithSchema() throws Exception
  {
    Map arvoInputFormatMap = ImmutableMap.of("type", "avro_ocf");
    doIndexTest(arvoInputFormatMap);
  }

  private void doIndexTest(Map arvoInputFormatMap) throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> sqlInputSourcePropsTransform = spec -> {
        try {
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_FILTER%%",
              "*.avro"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_BASE_DIR%%",
              "/resources/data/batch_index/avro"
          );
          spec = StringUtils.replace(
              spec,
              "%%AVRO_INPUT_FORMAT%%",
              jsonMapper.writeValueAsString(arvoInputFormatMap)
          );
          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          sqlInputSourcePropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}