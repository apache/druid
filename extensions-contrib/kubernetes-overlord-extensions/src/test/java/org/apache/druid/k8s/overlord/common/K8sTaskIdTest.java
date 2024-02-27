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

package org.apache.druid.k8s.overlord.common;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class K8sTaskIdTest
{
  @Test
  public void testModifyingTaskIDToBeK8sCompliant()
  {
    String original = "coordinator-issued_compact_k8smetrics_aeifmefd_2022-08-18T15:33:26.094Z";
    String result = new K8sTaskId(original).getK8sJobName();
    assertEquals("coordinatorissuedcompactk8smet-2e2c1862cb7ad1d01f4794b27a4438b0", result);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(K8sTaskId.class)
                  .withNonnullFields("k8sJobName", "originalTaskId")
                  .usingGetClass()
                  .verify();
  }
}
