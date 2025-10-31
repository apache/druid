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

package org.apache.druid.testing.embedded.indexer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.function.Function;

public class ITSystemTableBatchIndexTaskTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String SYSTEM_QUERIES_RESOURCE = "/indexer/sys_segment_batch_index_queries.json";

  @Test
  public void testIndexData() throws Exception
  {
    try (
        final Closeable ignored = unloader(dataSource)
    ) {

      final Function<String, String> transform = spec -> {
        try {
          return StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString("0")
          );
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTestSqlTest(
          dataSource,
          INDEX_TASK,
          SYSTEM_QUERIES_RESOURCE,
          transform
      );
    }
  }
}
