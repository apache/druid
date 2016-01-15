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

package io.druid.curator;

import io.druid.guice.JsonConfigTesterBase;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

public class CuratorConfigTest extends JsonConfigTesterBase<CuratorConfig>
{
  @Test
  public void testSerde() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    propertyValues.put(getPropertyKey("host"), "fooHost");
    propertyValues.put(getPropertyKey("acl"), "true");
    testProperties.putAll(propertyValues);
    configProvider.inject(testProperties, configurator);
    CuratorConfig config = configProvider.get().get();
    Assert.assertEquals("fooHost", config.getZkHosts());
    Assert.assertEquals(true, config.getEnableAcl());
  }
}
