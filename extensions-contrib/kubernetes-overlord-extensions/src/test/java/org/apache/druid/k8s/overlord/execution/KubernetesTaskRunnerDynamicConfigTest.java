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

package org.apache.druid.k8s.overlord.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class KubernetesTaskRunnerDynamicConfigTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    String json = "{\n"
                  + "  \"type\": \"default\",\n"
                  + "  \"podTemplateSelectStrategy\": {\n"
                  + "    \"type\": \"default\"\n"
                  + "  }\n"
                  + "}";

    KubernetesTaskRunnerDynamicConfig deserialized = jsonMapper.readValue(
        json,
        KubernetesTaskRunnerDynamicConfig.class
    );
    PodTemplateSelectStrategy selectStrategy = deserialized.getPodTemplateSelectStrategy();
    Assert.assertTrue(selectStrategy instanceof TaskTypePodTemplateSelectStrategy);

    json = "{\n"
           + "  \"type\": \"default\",\n"
           + "  \"podTemplateSelectStrategy\":\n"
           + "  {\n"
           + "    \"type\": \"selectorBased\",\n"
           + "    \"selectors\": [\n"
           + "      {\n"
           + "        \"selectionKey\": \"low-throughput\",\n"
           + "        \"context.tags\":\n"
           + "        {\n"
           + "          \"billingCategory\": [\"streaming_ingestion\"]\n"
           + "        },\n"
           + "        \"dataSource\": [\"wikipedia\"]\n"
           + "      },\n"
           + "      {\n"
           + "        \"selectionKey\": \"medium-throughput\",\n"
           + "        \"type\": [\"index_kafka\"]\n"
           + "      }\n"
           + "    ]\n"
           + "  }\n"
           + "}";

    deserialized = jsonMapper.readValue(json, KubernetesTaskRunnerDynamicConfig.class);
    selectStrategy = deserialized.getPodTemplateSelectStrategy();
    Assert.assertTrue(selectStrategy instanceof SelectorBasedPodTemplateSelectStrategy);
    Assert.assertEquals(2, ((SelectorBasedPodTemplateSelectStrategy) selectStrategy).getSelectors().size());
  }
}
