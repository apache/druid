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

import java.util.List;
import java.util.Properties;

public class ExhibitorConfigTest extends JsonConfigTesterBase<ExhibitorConfig>
{
  @Test
  public void testSerde()
  {
    propertyValues.put(getPropertyKey("hosts"), "[\"hostA\",\"hostB\"]");
    propertyValues.put(getPropertyKey("port"), "80");
    propertyValues.put(getPropertyKey("restUriPath"), "/list");
    propertyValues.put(getPropertyKey("useSsl"), "true");
    propertyValues.put(getPropertyKey("pollingMs"), "1000");
    testProperties.putAll(propertyValues);
    configProvider.inject(testProperties, configurator);
    ExhibitorConfig config = configProvider.get().get();

    List<String> hosts = config.getHosts();
    Assert.assertEquals(2, hosts.size());
    Assert.assertTrue(hosts.contains("hostA"));
    Assert.assertTrue(hosts.contains("hostB"));
    Assert.assertEquals(80, config.getRestPort());
    Assert.assertEquals("/list", config.getRestUriPath());
    Assert.assertTrue(config.getUseSsl());
    Assert.assertEquals(1000, config.getPollingMs());
  }

  @Test
  public void defaultValues()
  {
    configProvider.inject(new Properties(), configurator);
    ExhibitorConfig config = configProvider.get().get();

    List<String> hosts = config.getHosts();
    Assert.assertTrue(hosts.isEmpty());
    Assert.assertEquals(8080, config.getRestPort());
    Assert.assertEquals("/exhibitor/v1/cluster/list", config.getRestUriPath());
    Assert.assertFalse(config.getUseSsl());
    Assert.assertEquals(10000, config.getPollingMs());
  }
}
