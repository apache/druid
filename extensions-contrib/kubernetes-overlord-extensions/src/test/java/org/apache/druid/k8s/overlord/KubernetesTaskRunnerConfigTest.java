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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KubernetesTaskRunnerConfigTest
{
  @Test
  public void test_deserializable() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    KubernetesTaskRunnerConfig config = mapper.readValue(
        this.getClass().getClassLoader().getResource("kubernetesTaskRunnerConfig.json"),
        KubernetesTaskRunnerConfig.class
    );

    Assert.assertEquals("namespace", config.getNamespace());
    Assert.assertFalse(config.isDebugJobs());
    Assert.assertEquals("name", config.getPrimaryContainerName());
    Assert.assertEquals("karlkfi/kubexit:v0.3.2", config.getKubexitImage());
    Assert.assertNull(config.getGraceTerminationPeriodSeconds());
    Assert.assertTrue(config.isDisableClientProxy());
    Assert.assertEquals(new Period("PT4H"), config.getTaskTimeout());
    Assert.assertEquals(new Period("P2D"), config.getTaskCleanupDelay());
    Assert.assertEquals(new Period("PT10m"), config.getTaskCleanupInterval());
    Assert.assertEquals(new Period("PT1H"), config.getTaskLaunchTimeout());
    Assert.assertEquals(ImmutableList.of(), config.getPeonMonitors());
    Assert.assertEquals(ImmutableList.of(), config.getJavaOptsArray());
    Assert.assertEquals(ImmutableMap.of(), config.getLabels());
    Assert.assertEquals(ImmutableMap.of(), config.getAnnotations());
    Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getCapacity());
  }

  @Test
  public void test_builder_preservesDefaults()
  {
    KubernetesTaskRunnerConfig config = KubernetesTaskRunnerConfig.builder()
        .withNamespace("namespace")
        .withDisableClientProxy(true)
        .build();

    Assert.assertEquals("namespace", config.getNamespace());
    Assert.assertFalse(config.isDebugJobs());
    Assert.assertNull(config.getPrimaryContainerName());
    Assert.assertEquals("karlkfi/kubexit:v0.3.2", config.getKubexitImage());
    Assert.assertNull(config.getGraceTerminationPeriodSeconds());
    Assert.assertTrue(config.isDisableClientProxy());
    Assert.assertEquals(new Period("PT4H"), config.getTaskTimeout());
    Assert.assertEquals(new Period("P2D"), config.getTaskCleanupDelay());
    Assert.assertEquals(new Period("PT10m"), config.getTaskCleanupInterval());
    Assert.assertEquals(new Period("PT1H"), config.getTaskLaunchTimeout());
    Assert.assertEquals(ImmutableList.of(), config.getPeonMonitors());
    Assert.assertEquals(ImmutableList.of(), config.getJavaOptsArray());
    Assert.assertEquals(ImmutableMap.of(), config.getLabels());
    Assert.assertEquals(ImmutableMap.of(), config.getAnnotations());
    Assert.assertEquals(Integer.valueOf(Integer.MAX_VALUE), config.getCapacity());
  }

  @Test
  public void test_builder()
  {
    KubernetesTaskRunnerConfig config = KubernetesTaskRunnerConfig.builder()
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

    Assert.assertEquals("namespace", config.getNamespace());
    Assert.assertTrue(config.isDebugJobs());
    Assert.assertEquals("primary", config.getPrimaryContainerName());
    Assert.assertEquals("image", config.getKubexitImage());
    Assert.assertEquals(Long.valueOf(0), config.getGraceTerminationPeriodSeconds());
    Assert.assertTrue(config.isDisableClientProxy());
    Assert.assertEquals(new Period("PT2H"), config.getTaskTimeout());
    Assert.assertEquals(new Period("P1D"), config.getTaskCleanupDelay());
    Assert.assertEquals(new Period("PT1H"), config.getTaskCleanupInterval());
    Assert.assertEquals(new Period("PT1H"), config.getTaskLaunchTimeout());
    Assert.assertEquals(ImmutableList.of("monitor"), config.getPeonMonitors());
    Assert.assertEquals(ImmutableList.of("option"), config.getJavaOptsArray());
    Assert.assertEquals(ImmutableMap.of("key", "value"), config.getLabels());
    Assert.assertEquals(ImmutableMap.of("key", "value"), config.getAnnotations());
    Assert.assertEquals(Integer.valueOf(1), config.getCapacity());
  }
}
