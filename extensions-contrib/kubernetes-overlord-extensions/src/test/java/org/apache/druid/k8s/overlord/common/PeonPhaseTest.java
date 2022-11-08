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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeonPhaseTest
{

  @Test
  void testGetPhaseForToMakeCoverageHappy()
  {
    Pod pod = mock(Pod.class);
    PodStatus status = mock(PodStatus.class);
    when(status.getPhase()).thenReturn("Succeeded");
    when(pod.getStatus()).thenReturn(status);
    assertEquals(PeonPhase.UNKNOWN, PeonPhase.getPhaseFor(null));
    assertEquals(PeonPhase.SUCCEEDED, PeonPhase.getPhaseFor(pod));
  }
}
