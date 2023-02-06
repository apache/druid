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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum PeonPhase
{
  PENDING("Pending"),
  SUCCEEDED("Succeeded"),
  FAILED("Failed"),
  UNKNOWN("Unknown"),
  RUNNING("Running");

  private static final Map<String, PeonPhase> PHASE_MAP = Arrays.stream(PeonPhase.values())
                                                                .collect(Collectors.toMap(
                                                                    PeonPhase::getPhase,
                                                                    Function.identity()
                                                                ));
  private final String phase;

  PeonPhase(String phase)
  {
    this.phase = phase;
  }

  public String getPhase()
  {
    return phase;
  }

  public static PeonPhase getPhaseFor(Pod pod)
  {
    if (pod == null) {
      return UNKNOWN;
    }
    return PHASE_MAP.get(pod.getStatus().getPhase());
  }

}
