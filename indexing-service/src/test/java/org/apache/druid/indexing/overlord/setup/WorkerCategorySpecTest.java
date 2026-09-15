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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WorkerCategorySpecTest
{
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
  }

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"strong\": true,\n"
                     + "  \"categoryMap\": {\n"
                     + "    \"index_kafka\": {\"defaultCategory\": \"c1\", \"categoryAffinity\": {\"ds1\": \"c2\"}}\n"
                     + "  }\n"
                     + "}";

    WorkerCategorySpec workerCategorySpec = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                WorkerCategorySpec.class
            )
        ), WorkerCategorySpec.class
    );

    Assertions.assertTrue(workerCategorySpec.isStrong());
    Assertions.assertEquals(ImmutableMap.of(
        "index_kafka",
        new WorkerCategorySpec.CategoryConfig("c1", ImmutableMap.of("ds1", "c2"), null)
    ), workerCategorySpec.getCategoryMap());
  }
}
