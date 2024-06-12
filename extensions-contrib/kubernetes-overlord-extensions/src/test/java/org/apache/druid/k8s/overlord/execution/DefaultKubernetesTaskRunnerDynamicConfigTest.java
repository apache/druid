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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class DefaultKubernetesTaskRunnerDynamicConfigTest
{

  @Test
  public void getPodTemplateSelectStrategyTest()
  {
    PodTemplateSelectStrategy strategy = new TaskTypePodTemplateSelectStrategy();
    DefaultKubernetesTaskRunnerDynamicConfig config = new DefaultKubernetesTaskRunnerDynamicConfig(strategy);

    Assert.assertEquals(strategy, config.getPodTemplateSelectStrategy());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    PodTemplateSelectStrategy strategy = new TaskTypePodTemplateSelectStrategy();
    DefaultKubernetesTaskRunnerDynamicConfig config = new DefaultKubernetesTaskRunnerDynamicConfig(strategy);

    DefaultKubernetesTaskRunnerDynamicConfig config2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(config),
        DefaultKubernetesTaskRunnerDynamicConfig.class
    );
    Assert.assertEquals(config, config2);
  }
}
