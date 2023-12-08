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
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
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
import java.util.Collection;

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
      new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null),
      null,
      null,
      null,
      null,
      ImmutableMap.of("key", "val")
  );
  private static final DataSourceCompactionConfig NEW_CONFIG = new DataSourceCompactionConfig(
      "newDataSource",
      null,
      500L,
      null,
      new Period(1800),
      null,
      new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null),
      null,
      null,
      null,
      null,
      ImmutableMap.of("key", "val")
  );
  private static final byte[] OLD_CONFIG_IN_BYTES = {1, 2, 3};

  private static final CoordinatorCompactionConfig ORIGINAL_CONFIG
      = CoordinatorCompactionConfig.from(ImmutableList.of(OLD_CONFIG));

  private static final String DATASOURCE_NOT_EXISTS = "notExists";

  @Mock
  private JacksonConfigManager mockJacksonConfigManager;

  @Mock
  private HttpServletRequest mockHttpServletRequest;

  @Mock
  private MetadataStorageConnector mockConnector;

  @Mock
  private MetadataStorageTablesConfig mockConnectorConfig;

  @Mock
  private AuditManager mockAuditManager;

  private CoordinatorCompactionConfigsResource coordinatorCompactionConfigsResource;

  @Before
  public void setup()
  {
    Mockito.when(mockConnector.lookup(
                     ArgumentMatchers.anyString(),
                     ArgumentMatchers.eq("name"),
                     ArgumentMatchers.eq("payload"),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
                 )
    ).thenReturn(OLD_CONFIG_IN_BYTES);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
                     ArgumentMatchers.eq(OLD_CONFIG_IN_BYTES),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
                 )
    ).thenReturn(ORIGINAL_CONFIG);
    Mockito.when(mockConnectorConfig.getConfigTable()).thenReturn("druid_config");
    Mockito.when(mockAuditManager.fetchAuditHistory(
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     ArgumentMatchers.any()
                 )
    ).thenReturn(ImmutableList.of());
    coordinatorCompactionConfigsResource = new CoordinatorCompactionConfigsResource(
        new CoordinatorConfigManager(mockJacksonConfigManager, mockConnector, mockConnectorConfig),
        mockAuditManager
    );
    Mockito.when(mockHttpServletRequest.getRemoteAddr()).thenReturn("123");
  }

  @Test
  public void testSetCompactionTaskLimitWithExistingConfig()
  {
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(
        CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     oldConfigCaptor.capture(),
                     newConfigCaptor.capture(),
                     ArgumentMatchers.any()
                 )
    ).thenReturn(ConfigManager.SetResult.ok());

    double compactionTaskSlotRatio = 0.5;
    int maxCompactionTaskSlots = 9;
    Response result = coordinatorCompactionConfigsResource.setCompactionTaskLimit(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        true,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), OLD_CONFIG_IN_BYTES);
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(newConfigCaptor.getValue().getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
    Assert.assertTrue(newConfigCaptor.getValue().isUseAutoScaleSlots());
    Assert.assertEquals(compactionTaskSlotRatio, newConfigCaptor.getValue().getCompactionTaskSlotRatio(), 0);
  }

  @Test
  public void testAddOrUpdateCompactionConfigWithExistingConfig()
  {
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(
        CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     oldConfigCaptor.capture(),
                     newConfigCaptor.capture(),
                     ArgumentMatchers.any()
                 )
    ).thenReturn(ConfigManager.SetResult.ok());

    final DataSourceCompactionConfig newConfig = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, true),
        null,
        null,
        null,
        null,
        ImmutableMap.of("key", "val")
    );
    Response result = coordinatorCompactionConfigsResource.addOrUpdateCompactionConfig(
        newConfig,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), OLD_CONFIG_IN_BYTES);
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(2, newConfigCaptor.getValue().getCompactionConfigs().size());
    Assert.assertEquals(OLD_CONFIG, newConfigCaptor.getValue().getCompactionConfigs().get(0));
    Assert.assertEquals(newConfig, newConfigCaptor.getValue().getCompactionConfigs().get(1));
  }

  @Test
  public void testDeleteCompactionConfigWithExistingConfig()
  {
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(
        CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     oldConfigCaptor.capture(),
                     newConfigCaptor.capture(),
                     ArgumentMatchers.any()
                 )
    ).thenReturn(ConfigManager.SetResult.ok());
    final String datasourceName = "dataSource";
    final DataSourceCompactionConfig toDelete = new DataSourceCompactionConfig(
        datasourceName,
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null),
        null,
        null,
        null,
        null,
        ImmutableMap.of("key", "val")
    );
    final CoordinatorCompactionConfig originalConfig = CoordinatorCompactionConfig.from(ImmutableList.of(toDelete));
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
                     ArgumentMatchers.eq(OLD_CONFIG_IN_BYTES),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
                 )
    ).thenReturn(originalConfig);

    Response result = coordinatorCompactionConfigsResource.deleteCompactionConfig(
        datasourceName,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), OLD_CONFIG_IN_BYTES);
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(0, newConfigCaptor.getValue().getCompactionConfigs().size());
  }

  @Test
  public void testUpdateShouldRetryIfRetryableException()
  {
    Mockito.when(
        mockJacksonConfigManager.set(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )
    ).thenReturn(ConfigManager.SetResult.retryableFailure(new ISE("retryable")));

    coordinatorCompactionConfigsResource.addOrUpdateCompactionConfig(
        NEW_CONFIG,
        mockHttpServletRequest
    );

    // Verify that the update is retried upto the max number of retries
    Mockito.verify(
        mockJacksonConfigManager,
        Mockito.times(CoordinatorCompactionConfigsResource.UPDATE_NUM_RETRY)
    ).set(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
  }

  @Test
  public void testUpdateShouldNotRetryIfNotRetryableException()
  {
    Mockito.when(
        mockJacksonConfigManager.set(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )
    ).thenReturn(ConfigManager.SetResult.failure(new ISE("retryable")));

    coordinatorCompactionConfigsResource.addOrUpdateCompactionConfig(
        NEW_CONFIG,
        mockHttpServletRequest
    );

    // Verify that the update is tried only once
    Mockito.verify(mockJacksonConfigManager, Mockito.times(1)).set(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
  }

  @Test
  public void testSetCompactionTaskLimitWithoutExistingConfig()
  {
    Mockito.when(mockConnector.lookup(
                     ArgumentMatchers.anyString(),
                     ArgumentMatchers.eq("name"),
                     ArgumentMatchers.eq("payload"),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
                 )
    ).thenReturn(null);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
                     ArgumentMatchers.eq(null),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
                 )
    ).thenReturn(CoordinatorCompactionConfig.empty());
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(
        CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     oldConfigCaptor.capture(),
                     newConfigCaptor.capture(),
                     ArgumentMatchers.any()
                 )
    ).thenReturn(ConfigManager.SetResult.ok());

    double compactionTaskSlotRatio = 0.5;
    int maxCompactionTaskSlots = 9;
    Response result = coordinatorCompactionConfigsResource.setCompactionTaskLimit(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        true,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assert.assertNull(oldConfigCaptor.getValue());
    Assert.assertNotNull(newConfigCaptor.getValue());
    Assert.assertEquals(newConfigCaptor.getValue().getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
    Assert.assertTrue(newConfigCaptor.getValue().isUseAutoScaleSlots());
    Assert.assertEquals(compactionTaskSlotRatio, newConfigCaptor.getValue().getCompactionTaskSlotRatio(), 0);
  }

  @Test
  public void testAddOrUpdateCompactionConfigWithoutExistingConfig()
  {
    Mockito.when(mockConnector.lookup(
                     ArgumentMatchers.anyString(),
                     ArgumentMatchers.eq("name"),
                     ArgumentMatchers.eq("payload"),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
                 )
    ).thenReturn(null);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
                     ArgumentMatchers.eq(null),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
                 )
    ).thenReturn(CoordinatorCompactionConfig.empty());
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(
        CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
                     oldConfigCaptor.capture(),
                     newConfigCaptor.capture(),
                     ArgumentMatchers.any()
                 )
    ).thenReturn(ConfigManager.SetResult.ok());

    final DataSourceCompactionConfig newConfig = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null),
        null,
        null,
        null,
        null,
        ImmutableMap.of("key", "val")
    );
    String author = "maytas";
    String comment = "hello";
    Response result = coordinatorCompactionConfigsResource.addOrUpdateCompactionConfig(
        newConfig,
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
    Mockito.when(mockConnector.lookup(
                     ArgumentMatchers.anyString(),
                     ArgumentMatchers.eq("name"),
                     ArgumentMatchers.eq("payload"),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
                 )
    ).thenReturn(null);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
                     ArgumentMatchers.eq(null),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
                     ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
                 )
    ).thenReturn(CoordinatorCompactionConfig.empty());
    Response result = coordinatorCompactionConfigsResource.deleteCompactionConfig(
        DATASOURCE_NOT_EXISTS,
        mockHttpServletRequest
    );
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), result.getStatus());
  }

  @Test
  public void testGetCompactionConfigHistoryForUnknownDataSourceShouldReturnEmptyList()
  {
    Response response = coordinatorCompactionConfigsResource.getCompactionConfigHistory(
        DATASOURCE_NOT_EXISTS,
        null,
        null
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertTrue(((Collection) response.getEntity()).isEmpty());
  }
}
