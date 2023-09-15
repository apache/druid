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

package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KubernetesAndWorkerTaskRunnerConfigTest
{
  @Test
  public void test_deserializable() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    KubernetesAndWorkerTaskRunnerConfig config = mapper.readValue(
        this.getClass().getClassLoader().getResource("kubernetesAndWorkerTaskRunnerConfig.json"),
        KubernetesAndWorkerTaskRunnerConfig.class
    );

    Assert.assertEquals(RemoteTaskRunnerFactory.TYPE_NAME, config.getWorkerTaskRunnerType());
    Assert.assertFalse(config.isSendAllTasksToWorkerTaskRunner());

  }

  @Test
  public void test_withDefaults()
  {
    KubernetesAndWorkerTaskRunnerConfig config = new KubernetesAndWorkerTaskRunnerConfig(null, null);

    Assert.assertEquals(HttpRemoteTaskRunnerFactory.TYPE_NAME, config.getWorkerTaskRunnerType());
    Assert.assertFalse(config.isSendAllTasksToWorkerTaskRunner());
  }
}
