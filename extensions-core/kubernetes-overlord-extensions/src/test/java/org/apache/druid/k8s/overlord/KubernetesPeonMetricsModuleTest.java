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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.server.emitter.ExtraServiceDimensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Map;

public class KubernetesPeonMetricsModuleTest
{
  private static final Key<Map<String, String>> EXTRA_DIMENSIONS_KEY =
      Key.get(new TypeLiteral<>() {}, ExtraServiceDimensions.class);

  @Test
  public void test_podTemplateEnvSet_addsDimension()
  {
    Assertions.assertEquals(
        Map.of(DruidK8sConstants.POD_TEMPLATE_DIMENSION, "podSpec1"),
        extraDimensions("podSpec1")
    );
  }

  @Test
  public void test_podTemplateEnvUnset_addsNoDimension()
  {
    Assertions.assertEquals(Map.of(), extraDimensions(null));
  }

  @Test
  public void test_podTemplateEnvEmpty_addsNoDimension()
  {
    Assertions.assertEquals(Map.of(), extraDimensions(""));
  }

  private static Map<String, String> extraDimensions(@Nullable String podTemplateName)
  {
    Injector injector = Guice.createInjector(new KubernetesPeonMetricsModule()
    {
      @Override
      String getPodTemplateName()
      {
        return podTemplateName;
      }
    });
    return injector.getInstance(EXTRA_DIMENSIONS_KEY);
  }
}
