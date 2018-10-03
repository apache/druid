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

package org.apache.druid.curator;

import org.apache.druid.guice.JsonConfigTesterBase;
import org.junit.Assert;
import org.junit.Test;

public class CuratorConfigTest extends JsonConfigTesterBase<CuratorConfig>
{
  @Test
  public void testSerde()
  {
    propertyValues.put(getPropertyKey("host"), "fooHost");
    propertyValues.put(getPropertyKey("acl"), "true");
    propertyValues.put(getPropertyKey("user"), "test-zk-user");
    propertyValues.put(getPropertyKey("pwd"), "test-zk-pwd");
    propertyValues.put(getPropertyKey("authScheme"), "auth");
    testProperties.putAll(propertyValues);
    configProvider.inject(testProperties, configurator);
    CuratorConfig config = configProvider.get().get();
    Assert.assertEquals("fooHost", config.getZkHosts());
    Assert.assertEquals(true, config.getEnableAcl());
    Assert.assertEquals("test-zk-user", config.getZkUser());
    Assert.assertEquals("test-zk-pwd", config.getZkPwd());
    Assert.assertEquals("auth", config.getAuthScheme());
  }
}
