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
           + "   \"type\":\"default\",\n"
           + "   \"podTemplateSelectStrategy\":{\n"
           + "      \"type\":\"taskProperties\",\n"
           + "      \"templateSelectors\":[\n"
           + "         {\n"
           + "            \"templateKey\":\"low-throughput\",\n"
           + "            \"matcher\":{\n"
           + "               \"type\":\"default\",\n"
           + "               \"context.tags\":{\n"
           + "                  \"billingCategory\":[\n"
           + "                     \"streaming_ingestion\"\n"
           + "                  ]\n"
           + "               },\n"
           + "               \"task\":{\n"
           + "                  \"datasource\":[\n"
           + "                     \"wikipedia\"\n"
           + "                  ]\n"
           + "               }\n"
           + "            }\n"
           + "         },\n"
           + "         {\n"
           + "            \"templateKey\":\"medium-throughput\",\n"
           + "            \"matcher\":{\n"
           + "               \"type\":\"default\",\n"
           + "               \"task\":{\n"
           + "                  \"type\":[\n"
           + "                     \"index_kafka\"\n"
           + "                  ]\n"
           + "               }\n"
           + "            }\n"
           + "         }\n"
           + "      ]\n"
           + "   }\n"
           + "}";

    deserialized = jsonMapper.readValue(json, KubernetesTaskRunnerDynamicConfig.class);
    selectStrategy = deserialized.getPodTemplateSelectStrategy();
    Assert.assertTrue(selectStrategy instanceof TaskPropertiesPodTemplateSelectStrategy);
    Assert.assertEquals(2, ((TaskPropertiesPodTemplateSelectStrategy) selectStrategy).getTemplateSelectors().size());
  }
}
