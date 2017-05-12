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

package io.druid.server.http;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.druid.common.config.JacksonConfigManager;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.server.coordinator.CoordinatorDynamicConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class CoordinatorDynamicConfigTest
{
  private JacksonConfigManager configManager;
  private ObjectMapper mapper;

  @Before
  public void setup()
  {
    mapper = new DefaultObjectMapper();
    configManager = EasyMock.mock(JacksonConfigManager.class);
    EasyMock.expect(
        configManager.watch(
            CoordinatorDynamicConfig.CONFIG_KEY,
            CoordinatorDynamicConfig.class
        )
    ).andReturn(new AtomicReference<>(null)).anyTimes();
    EasyMock.replay(configManager);
    InjectableValues inject = new InjectableValues.Std().addValue(JacksonConfigManager.class, configManager);
    mapper.setInjectableValues(inject);
  }

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"]\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );

    Assert.assertEquals(
        new CoordinatorDynamicConfig(1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of("test1", "test2"), false),
        actual
    );
  }

  @Test
  public void testSerdeWithStringinKillDataSourceWhitelist() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killDataSourceWhitelist\": \" test1 ,test2 \"\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );

    Assert.assertEquals(
        new CoordinatorDynamicConfig(1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of("test1", "test2"), false),
        actual
    );
  }

  @Test
  public void testSerdeWithKillAllDataSources() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killAllDataSources\": true\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );

    Assert.assertEquals(
        new CoordinatorDynamicConfig(1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of(), true),
        actual
    );



    //ensure whitelist is empty when killAllDataSources is true
    try {
      jsonStr = "{\n"
                + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                + "  \"killAllDataSources\": true\n"
                + "}\n";
      mapper.readValue(
          jsonStr,
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    } catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IAE);
    }
  }

  @Test
  public void testBuilderDefaults()
  {
    Assert.assertEquals(
        new CoordinatorDynamicConfig(900000, 524288000, 100, 5, 15, 10, 1, false, null, false),
        new CoordinatorDynamicConfig.Builder().build()
    );
  }

  @Test
  public void testUpdate()
  {
    CoordinatorDynamicConfig current = new CoordinatorDynamicConfig(99, 99, 99, 99, 99, 99, 99, true, ImmutableSet.of("x"), false);
    JacksonConfigManager mock = EasyMock.mock(JacksonConfigManager.class);
    EasyMock.expect(mock.watch(CoordinatorDynamicConfig.CONFIG_KEY, CoordinatorDynamicConfig.class)).andReturn(
        new AtomicReference<>(current)
    );
    EasyMock.replay(mock);
    Assert.assertEquals(
        current,
        new CoordinatorDynamicConfig(mock, null, null, null, null, null, null, null, null, null, null)
    );
  }

  @Test
  public void testEqualsAndHashCodeSanity()
  {
    CoordinatorDynamicConfig config1 = new CoordinatorDynamicConfig(900000, 524288000, 100, 5, 15, 10, 1, false, null, false);
    CoordinatorDynamicConfig config2 = new CoordinatorDynamicConfig(900000, 524288000, 100, 5, 15, 10, 1, false, null, false);

    Assert.assertEquals(config1, config2);
    Assert.assertEquals(config1.hashCode(), config2.hashCode());
  }
}
