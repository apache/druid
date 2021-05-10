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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(MockitoJUnitRunner.class)
public class CoordinatorCompactionConfigsResourceTest
{
  private static final DataSourceCompactionConfig OLD_CONFIG = new DataSourceCompactionConfig(
      "oldDataSource",
      null,
      500L,
      null,
      new Period(3600),
      null,
      new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
      null,
      ImmutableMap.of("key", "val")
  );
  private static final CoordinatorCompactionConfig ORIGINAL_CONFIG = CoordinatorCompactionConfig.from(ImmutableList.of(OLD_CONFIG));

  @Mock
  private JacksonConfigManager mockJacksonConfigManager;

  @Mock
  private HttpServletRequest mockHttpServletRequest;

  private CoordinatorCompactionConfigsResource coordinatorCompactionConfigsResource;

  @Before
  public void setup()
  {
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(ORIGINAL_CONFIG));
    coordinatorCompactionConfigsResource = new CoordinatorCompactionConfigsResource(mockJacksonConfigManager);
    Mockito.when(mockHttpServletRequest.getRemoteAddr()).thenReturn("123");
  }

  @Test
  public void testSetCompactionTaskLimitWithExistingConfig()
  {
    final ArgumentCaptor<CoordinatorCompactionConfig> oldConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    double compactionTaskSlotRatio = 0.5;
    int maxCompactionTaskSlots = 9;
    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.setCompactionTaskLimit(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        author,
        comment,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), ORIGINAL_CONFIG);
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(newConfigCaptor.getValue().getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
    Assert.assertEquals(compactionTaskSlotRatio, newConfigCaptor.getValue().getCompactionTaskSlotRatio(), 0);
  }

  @Test
  public void testAddOrUpdateCompactionConfigWithExistingConfig()
  {
    final ArgumentCaptor<CoordinatorCompactionConfig> oldConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    final DataSourceCompactionConfig newConfig = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
        null,
        ImmutableMap.of("key", "val")
    );
    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.addOrUpdateCompactionConfig(
        newConfig,
        author,
        comment,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), ORIGINAL_CONFIG);
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(2, newConfigCaptor.getValue().getCompactionConfigs().size());
    Assert.assertEquals(OLD_CONFIG, newConfigCaptor.getValue().getCompactionConfigs().get(0));
    Assert.assertEquals(newConfig, newConfigCaptor.getValue().getCompactionConfigs().get(1));
  }

  @Test
  public void testDeleteCompactionConfigWithExistingConfig()
  {
    final ArgumentCaptor<CoordinatorCompactionConfig> oldConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());
    final String datasourceName = "dataSource";
    final DataSourceCompactionConfig toDelete = new DataSourceCompactionConfig(
        datasourceName,
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
        null,
        ImmutableMap.of("key", "val")
    );
    final CoordinatorCompactionConfig originalConfig = CoordinatorCompactionConfig.from(ImmutableList.of(toDelete));
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(originalConfig));

    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.deleteCompactionConfig(
        datasourceName,
        author,
        comment,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), originalConfig);
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(0, newConfigCaptor.getValue().getCompactionConfigs().size());
  }

  @Test
  public void testUpdateConfigHelperShouldRetryIfRetryableException()
  {
    MutableInt nunCalled = new MutableInt(0);
    Callable<ConfigManager.SetResult> callable = () -> {
      nunCalled.increment();
      return ConfigManager.SetResult.fail(new Exception(), true);
    };
    coordinatorCompactionConfigsResource.updateConfigHelper(callable);
    Assert.assertEquals(CoordinatorCompactionConfigsResource.UPDATE_NUM_RETRY, (int) nunCalled.getValue());
  }

  @Test
  public void testUpdateConfigHelperShouldNotRetryIfNotRetryableException()
  {
    MutableInt nunCalled = new MutableInt(0);
    Callable<ConfigManager.SetResult> callable = () -> {
      nunCalled.increment();
      return ConfigManager.SetResult.fail(new Exception(), false);
    };
    coordinatorCompactionConfigsResource.updateConfigHelper(callable);
    Assert.assertEquals(1, (int) nunCalled.getValue());
  }

  @Test
  public void testSetCompactionTaskLimitWithoutExistingConfig()
  {
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty()));
    final ArgumentCaptor<CoordinatorCompactionConfig> oldConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    double compactionTaskSlotRatio = 0.5;
    int maxCompactionTaskSlots = 9;
    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.setCompactionTaskLimit(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        author,
        comment,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNull(oldConfigCaptor.getValue());
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(newConfigCaptor.getValue().getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
    Assert.assertEquals(compactionTaskSlotRatio, newConfigCaptor.getValue().getCompactionTaskSlotRatio(), 0);
  }

  @Test
  public void testAddOrUpdateCompactionConfigWithoutExistingConfig()
  {
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty()));
    final ArgumentCaptor<CoordinatorCompactionConfig> oldConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    final DataSourceCompactionConfig newConfig = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
        null,
        ImmutableMap.of("key", "val")
    );
    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.addOrUpdateCompactionConfig(
        newConfig,
        author,
        comment,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNull(oldConfigCaptor.getValue());
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(1, newConfigCaptor.getValue().getCompactionConfigs().size());
    Assert.assertEquals(newConfig, newConfigCaptor.getValue().getCompactionConfigs().get(0));
  }

  @Test
  public void testDeleteCompactionConfigWithoutExistingConfigShouldFailAsDatasourceNotExist()
  {
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty()));
    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.deleteCompactionConfig(
        "notExist",
        author,
        comment,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), result.getStatus());
  }
}
