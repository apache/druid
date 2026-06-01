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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class KubernetesTaskRunnerStaticConfigTest
{
  @Test
  public void test_deserializable() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    KubernetesTaskRunnerStaticConfig config = mapper.readValue(
        this.getClass().getClassLoader().getResource("kubernetesTaskRunnerConfig.json"),
        KubernetesTaskRunnerStaticConfig.class
    );

    Assertions.assertEquals("namespace", config.getNamespace());
    Assertions.assertFalse(config.isDebugJobs());
    Assertions.assertEquals("name", config.getPrimaryContainerName());
    Assertions.assertEquals("karlkfi/kubexit:v0.3.2", config.getKubexitImage());
    Assertions.assertNull(config.getGraceTerminationPeriodSeconds());
    Assertions.assertTrue(config.isDisableClientProxy());
    Assertions.assertEquals(new Period("PT4H"), config.getTaskTimeout());
    Assertions.assertEquals(new Period("P2D"), config.getTaskCleanupDelay());
    Assertions.assertEquals(new Period("PT10m"), config.getTaskCleanupInterval());
    Assertions.assertEquals(new Period("PT1H"), config.getTaskLaunchTimeout());
    Assertions.assertEquals(ImmutableList.of(), config.getPeonMonitors());
    Assertions.assertEquals(ImmutableList.of(), config.getJavaOptsArray());
    Assertions.assertEquals(ImmutableMap.of(), config.getLabels());
    Assertions.assertEquals(ImmutableMap.of(), config.getAnnotations());
    Assertions.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getCapacity());
  }

  @Test
  public void test_builder_preservesDefaults()
  {
    KubernetesTaskRunnerStaticConfig config = KubernetesTaskRunnerConfig.builder()
        .withNamespace("namespace")
        .withDisableClientProxy(true)
        .build();

    Assertions.assertEquals("namespace", config.getNamespace());
    Assertions.assertFalse(config.isDebugJobs());
    Assertions.assertNull(config.getPrimaryContainerName());
    Assertions.assertEquals("karlkfi/kubexit:v0.3.2", config.getKubexitImage());
    Assertions.assertNull(config.getGraceTerminationPeriodSeconds());
    Assertions.assertTrue(config.isDisableClientProxy());
    Assertions.assertEquals(new Period("PT4H"), config.getTaskTimeout());
    Assertions.assertEquals(new Period("P2D"), config.getTaskCleanupDelay());
    Assertions.assertEquals(new Period("PT10m"), config.getTaskCleanupInterval());
    Assertions.assertEquals(new Period("PT1H"), config.getTaskLaunchTimeout());
    Assertions.assertEquals(ImmutableList.of(), config.getPeonMonitors());
    Assertions.assertEquals(ImmutableList.of(), config.getJavaOptsArray());
    Assertions.assertEquals(ImmutableMap.of(), config.getLabels());
    Assertions.assertEquals(ImmutableMap.of(), config.getAnnotations());
    Assertions.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getCapacity());
  }

  @Test
  public void test_builder()
  {
    KubernetesTaskRunnerStaticConfig config = KubernetesTaskRunnerConfig.builder()
        .withNamespace("namespace")
        .withDebugJob(true)
        .withSidecarSupport(true)
        .withPrimaryContainerName("primary")
        .withKubexitImage("image")
        .withGraceTerminationPeriodSeconds(0L)
        .withDisableClientProxy(true)
        .withTaskTimeout(new Period("PT2H"))
        .withTaskCleanupDelay(new Period("P1D"))
        .withTaskCleanupInterval(new Period("PT1H"))
        .withK8sJobLaunchTimeout(new Period("PT1H"))
        .withPeonMonitors(ImmutableList.of("monitor"))
        .withJavaOptsArray(ImmutableList.of("option"))
        .withLabels(ImmutableMap.of("key", "value"))
        .withAnnotations(ImmutableMap.of("key", "value"))
        .withCapacity(1)
        .build();

    Assertions.assertEquals("namespace", config.getNamespace());
    Assertions.assertTrue(config.isDebugJobs());
    Assertions.assertEquals("primary", config.getPrimaryContainerName());
    Assertions.assertEquals("image", config.getKubexitImage());
    Assertions.assertEquals(Long.valueOf(0), config.getGraceTerminationPeriodSeconds());
    Assertions.assertTrue(config.isDisableClientProxy());
    Assertions.assertEquals(new Period("PT2H"), config.getTaskTimeout());
    Assertions.assertEquals(new Period("P1D"), config.getTaskCleanupDelay());
    Assertions.assertEquals(new Period("PT1H"), config.getTaskCleanupInterval());
    Assertions.assertEquals(new Period("PT1H"), config.getTaskLaunchTimeout());
    Assertions.assertEquals(ImmutableList.of("monitor"), config.getPeonMonitors());
    Assertions.assertEquals(ImmutableList.of("option"), config.getJavaOptsArray());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), config.getLabels());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), config.getAnnotations());
    Assertions.assertEquals(Integer.valueOf(1), config.getCapacity());
  }
}
