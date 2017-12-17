/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.guice;

import com.google.inject.Injector;
import io.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.Test;

public class NullHandlingHelperInjectionTest
{
  public static String NULL_HANDLING_CONFIG_STRING = ("druid.generic.useDefaultValueForNull");

  @Test
  public void testNullHandlingHelperUseDefaultValues()
  {
    String prev = System.getProperty(NULL_HANDLING_CONFIG_STRING);
    try {
      System.setProperty(NULL_HANDLING_CONFIG_STRING, "true");
      Injector injector = GuiceInjectors.makeStartupInjector();
      Assert.assertEquals(true, NullHandling.useDefaultValuesForNull());
    }
    finally {
      if (prev != null) {
        resetNullHandlingHelper(prev);
      }
    }
  }

  @Test
  public void testNullHandlingHelperNoDefaultValues()
  {
    String prev = System.getProperty(NULL_HANDLING_CONFIG_STRING);
    try {
      System.setProperty(NULL_HANDLING_CONFIG_STRING, "false");
      Injector injector = GuiceInjectors.makeStartupInjector();
      Assert.assertEquals(false, NullHandling.useDefaultValuesForNull());
    }
    finally {
      if (prev != null) {
        resetNullHandlingHelper(prev);
      }
    }
  }

  private void resetNullHandlingHelper(String prev)
  {
    System.setProperty(NULL_HANDLING_CONFIG_STRING, prev);
    Injector injector = GuiceInjectors.makeStartupInjector();
    Assert.assertEquals(Boolean.valueOf(prev), NullHandling.useDefaultValuesForNull());
  }

}
