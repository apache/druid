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

import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.api.model.PodTemplateBuilder;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateWithName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PodTemplateWithNameTest
{
  @Test
  void testEqualityToMakeCoverageHappy()
  {
    PodTemplateWithName podTemplateWithName = new PodTemplateWithName(
        "name",
        new PodTemplateBuilder().build()
    );
    PodTemplateWithName podTemplateWithName2 = podTemplateWithName;

    Assertions.assertEquals(podTemplateWithName, podTemplateWithName2);
    Assertions.assertNotEquals(podTemplateWithName, null);
    Assertions.assertNotEquals(podTemplateWithName, "string");
    Assertions.assertEquals(podTemplateWithName.hashCode(), podTemplateWithName2.hashCode());
  }

  @Test
  void testGettersToMakeCoverageHappy()
  {
    String name = "name";
    PodTemplate podTemplate = new PodTemplateBuilder().build();
    PodTemplateWithName podTemplateWithName = new PodTemplateWithName(
        name,
        podTemplate
    );

    Assertions.assertEquals(name, podTemplateWithName.getName());
    Assertions.assertEquals(podTemplate, podTemplateWithName.getPodTemplate());
  }
}
