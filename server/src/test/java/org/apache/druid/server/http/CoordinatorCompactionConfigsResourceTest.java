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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.common.config.TestConfigManagerConfig;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestMetadataStorageConnector;
import org.apache.druid.metadata.TestMetadataStorageTablesConfig;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfigAuditEntry;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

@RunWith(MockitoJUnitRunner.class)
public class CoordinatorCompactionConfigsResourceTest
{
  private static final double DELTA = 1e-9;
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Mock
  private HttpServletRequest mockHttpServletRequest;

  private TestCoordinatorConfigManager configManager;
  private CoordinatorCompactionConfigsResource resource;

  @Before
  public void setup()
  {
    Mockito.when(mockHttpServletRequest.getRemoteAddr()).thenReturn("123");
    final AuditManager auditManager = new TestAuditManager();
    configManager = TestCoordinatorConfigManager.create(auditManager);
    resource = new CoordinatorCompactionConfigsResource(configManager, auditManager);
    configManager.delegate.start();
  }

  @After
  public void tearDown()
  {
    configManager.delegate.stop();
  }

  @Test
  public void testGetDefaultClusterConfig()
  {
    Response response = resource.getCompactionConfig();
    final DruidCompactionConfig defaultConfig
        = verifyAndGetPayload(response, DruidCompactionConfig.class);

    Assert.assertEquals(0.1, defaultConfig.getCompactionTaskSlotRatio(), DELTA);
    Assert.assertEquals(Integer.MAX_VALUE, defaultConfig.getMaxCompactionTaskSlots());
    Assert.assertFalse(defaultConfig.isUseAutoScaleSlots());
    Assert.assertTrue(defaultConfig.getCompactionConfigs().isEmpty());
  }

  @Test
  public void testUpdateClusterConfig()
  {
    Response response = resource.updateClusterCompactionConfig(
        new ClusterCompactionConfig(0.5, 10, true, null),
        mockHttpServletRequest
    );
    verifyStatus(Response.Status.OK, response);

    final DruidCompactionConfig updatedConfig = verifyAndGetPayload(
        resource.getCompactionConfig(),
        DruidCompactionConfig.class
    );

    Assert.assertNotNull(updatedConfig);
    Assert.assertEquals(0.5, updatedConfig.getCompactionTaskSlotRatio(), DELTA);
    Assert.assertEquals(10, updatedConfig.getMaxCompactionTaskSlots());
    Assert.assertTrue(updatedConfig.isUseAutoScaleSlots());
  }

  @Test
  public void testSetCompactionTaskLimit()
  {
    final DruidCompactionConfig defaultConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);

    Response response = resource.setCompactionTaskLimit(0.5, 9, true, mockHttpServletRequest);
    verifyStatus(Response.Status.OK, response);

    final DruidCompactionConfig updatedConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);

    // Verify that the task slot fields have been updated
    Assert.assertEquals(0.5, updatedConfig.getCompactionTaskSlotRatio(), DELTA);
    Assert.assertEquals(9, updatedConfig.getMaxCompactionTaskSlots());
    Assert.assertTrue(updatedConfig.isUseAutoScaleSlots());

    // Verify that the other fields are unchanged
    Assert.assertEquals(defaultConfig.getCompactionConfigs(), updatedConfig.getCompactionConfigs());
  }

  @Test
  public void testGetUnknownDatasourceConfigThrowsNotFound()
  {
    Response response = resource.getDatasourceCompactionConfig(TestDataSource.WIKI);
    verifyStatus(Response.Status.NOT_FOUND, response);
  }

  @Test
  public void testAddDatasourceConfig()
  {
    final DataSourceCompactionConfig newDatasourceConfig
        = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();
    Response response = resource.addOrUpdateDatasourceCompactionConfig(newDatasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.OK, response);

    final DataSourceCompactionConfig fetchedDatasourceConfig
        = verifyAndGetPayload(resource.getDatasourceCompactionConfig(TestDataSource.WIKI), DataSourceCompactionConfig.class);
    Assert.assertEquals(newDatasourceConfig, fetchedDatasourceConfig);

    final DruidCompactionConfig fullCompactionConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);
    Assert.assertEquals(1, fullCompactionConfig.getCompactionConfigs().size());
    Assert.assertEquals(newDatasourceConfig, fullCompactionConfig.getCompactionConfigs().get(0));
  }

  @Test
  public void testAddDatasourceConfigWithMSQEngineIsInvalid()
  {
    final DataSourceCompactionConfig newDatasourceConfig
        = DataSourceCompactionConfig.builder()
                                    .forDataSource(TestDataSource.WIKI)
                                    .withEngine(CompactionEngine.MSQ)
                                    .build();
    Response response = resource.addOrUpdateDatasourceCompactionConfig(newDatasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.BAD_REQUEST, response);
    Assert.assertTrue(response.getEntity() instanceof ErrorResponse);
    Assert.assertEquals(
        "MSQ engine in compaction config only supported with supervisor-based compaction on the Overlord.",
        ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage()
    );
  }

  @Test
  public void testUpdateDatasourceConfig()
  {
    final DataSourceCompactionConfig originalDatasourceConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(Period.hours(1))
        .withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, true)
        )
        .withEngine(CompactionEngine.NATIVE)
        .build();

    Response response = resource.addOrUpdateDatasourceCompactionConfig(
        originalDatasourceConfig,
        mockHttpServletRequest
    );
    verifyStatus(Response.Status.OK, response);

    final DataSourceCompactionConfig updatedDatasourceConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withInputSegmentSizeBytes(1000L)
        .withSkipOffsetFromLatest(Period.hours(3))
        .withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false)
        )
        .withEngine(CompactionEngine.MSQ)
        .build();

    response = resource.addOrUpdateDatasourceCompactionConfig(updatedDatasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.BAD_REQUEST, response);

    final DataSourceCompactionConfig latestDatasourceConfig
        = verifyAndGetPayload(resource.getDatasourceCompactionConfig(TestDataSource.WIKI), DataSourceCompactionConfig.class);
    Assert.assertEquals(originalDatasourceConfig, latestDatasourceConfig);

    final DruidCompactionConfig fullCompactionConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);
    Assert.assertEquals(1, fullCompactionConfig.getCompactionConfigs().size());
    Assert.assertEquals(originalDatasourceConfig, fullCompactionConfig.getCompactionConfigs().get(0));
  }

  @Test
  public void testDeleteDatasourceConfig()
  {
    final DataSourceCompactionConfig datasourceConfig
        = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();
    Response response = resource.addOrUpdateDatasourceCompactionConfig(datasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.OK, response);

    response = resource.deleteCompactionConfig(TestDataSource.WIKI, mockHttpServletRequest);
    verifyStatus(Response.Status.OK, response);

    response = resource.getDatasourceCompactionConfig(TestDataSource.WIKI);
    verifyStatus(Response.Status.NOT_FOUND, response);
  }

  @Test
  public void testDeleteUnknownDatasourceConfigThrowsNotFound()
  {
    Response response = resource.deleteCompactionConfig(TestDataSource.WIKI, mockHttpServletRequest);
    verifyStatus(Response.Status.NOT_FOUND, response);
  }

  @Test
  public void testUpdateIsRetriedIfFailureIsRetryable()
  {
    configManager.configUpdateResult
        = ConfigManager.SetResult.retryableFailure(new Exception("retryable"));
    resource.addOrUpdateDatasourceCompactionConfig(
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        mockHttpServletRequest
    );

    Assert.assertEquals(
        CoordinatorCompactionConfigsResource.MAX_UPDATE_RETRIES,
        configManager.numUpdateAttempts
    );
  }

  @Test
  public void testUpdateIsNotRetriedIfFailureIsNotRetryable()
  {
    configManager.configUpdateResult
        = ConfigManager.SetResult.failure(new Exception("not retryable"));
    resource.addOrUpdateDatasourceCompactionConfig(
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        mockHttpServletRequest
    );

    Assert.assertEquals(1, configManager.numUpdateAttempts);
  }

  @Test
  public void testGetDatasourceConfigHistory()
  {
    final DataSourceCompactionConfig.Builder builder
        = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI);

    final DataSourceCompactionConfig configV1 = builder.build();
    resource.addOrUpdateDatasourceCompactionConfig(configV1, mockHttpServletRequest);

    final DataSourceCompactionConfig configV2 = builder.withEngine(CompactionEngine.NATIVE).build();
    resource.addOrUpdateDatasourceCompactionConfig(configV2, mockHttpServletRequest);

    final DataSourceCompactionConfig configV3 = builder
        .withEngine(CompactionEngine.NATIVE)
        .withSkipOffsetFromLatest(Period.hours(1))
        .build();
    resource.addOrUpdateDatasourceCompactionConfig(configV3, mockHttpServletRequest);

    Response response = resource.getCompactionConfigHistory(TestDataSource.WIKI, null, null);
    verifyStatus(Response.Status.OK, response);

    final List<DataSourceCompactionConfigAuditEntry> history
        = (List<DataSourceCompactionConfigAuditEntry>) response.getEntity();
    Assert.assertEquals(3, history.size());
    Assert.assertEquals(configV1, history.get(0).getCompactionConfig());
    Assert.assertEquals(configV2, history.get(1).getCompactionConfig());
    Assert.assertEquals(configV3, history.get(2).getCompactionConfig());
  }

  @Test
  public void testGetHistoryOfUnknownDatasourceReturnsEmpty()
  {
    Response response = resource.getCompactionConfigHistory(TestDataSource.WIKI, null, null);
    verifyStatus(Response.Status.OK, response);
    Assert.assertTrue(((List<?>) response.getEntity()).isEmpty());
  }

  @Test
  public void testAddInvalidDatasourceConfigThrowsBadRequest()
  {
    final DataSourceCompactionConfig datasourceConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTaskContext(Collections.singletonMap(ClientMSQContext.CTX_MAX_NUM_TASKS, 1))
        .withEngine(CompactionEngine.MSQ)
        .build();

    final Response response = resource.addOrUpdateDatasourceCompactionConfig(datasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.BAD_REQUEST, response);
    Assert.assertTrue(response.getEntity() instanceof ErrorResponse);
    Assert.assertEquals(
        "MSQ engine in compaction config only supported with supervisor-based compaction on the Overlord.",
        ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage()
    );
  }
  @SuppressWarnings("unchecked")
  private <T> T verifyAndGetPayload(Response response, Class<T> type)
  {
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    Assert.assertTrue(type.isInstance(response.getEntity()));
    return (T) response.getEntity();
  }

  private void verifyStatus(Response.Status expectedStatus, Response response)
  {
    Assert.assertEquals(expectedStatus.getStatusCode(), response.getStatus());
  }

  /**
   * Test implementation of AuditManager that keeps audit entries in memory.
   */
  private static class TestAuditManager implements AuditManager
  {
    private final List<AuditEntry> audits = new ArrayList<>();

    @Override
    public void doAudit(AuditEntry event, Handle handle)
    {
      // do nothing
    }

    @Override
    public void doAudit(AuditEntry event)
    {
      final String json;
      try {
        json = OBJECT_MAPPER.writeValueAsString(event.getPayload().raw());
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      final AuditEntry eventWithSerializedPayload
          = AuditEntry.builder()
                      .key(event.getKey())
                      .type(event.getType())
                      .auditInfo(event.getAuditInfo())
                      .auditTime(event.getAuditTime())
                      .request(event.getRequest())
                      .serializedPayload(json)
                      .build();
      audits.add(eventWithSerializedPayload);
    }

    @Override
    public List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval)
    {
      return audits;
    }

    @Override
    public List<AuditEntry> fetchAuditHistory(String type, int limit)
    {
      return audits;
    }

    @Override
    public List<AuditEntry> fetchAuditHistory(String type, Interval interval)
    {
      return audits;
    }

    @Override
    public List<AuditEntry> fetchAuditHistory(String key, String type, int limit)
    {
      return audits;
    }

    @Override
    public int removeAuditLogsOlderThan(long timestamp)
    {
      return 0;
    }
  }

  /**
   * Test implementation of CoordinatorConfigManager to track number of update attempts.
   */
  private static class TestCoordinatorConfigManager extends CoordinatorConfigManager
  {
    private final ConfigManager delegate;
    private int numUpdateAttempts;
    private ConfigManager.SetResult configUpdateResult;

    static TestCoordinatorConfigManager create(AuditManager auditManager)
    {
      final MetadataStorageTablesConfig tablesConfig = new TestMetadataStorageTablesConfig()
      {
        @Override
        public String getConfigTable()
        {
          return "druid_config";
        }
      };

      final TestDBConnector dbConnector = new TestDBConnector();
      final ConfigManager configManager = new ConfigManager(
          dbConnector,
          Suppliers.ofInstance(tablesConfig),
          Suppliers.ofInstance(new TestConfigManagerConfig())
      );

      return new TestCoordinatorConfigManager(
          new JacksonConfigManager(configManager, OBJECT_MAPPER, auditManager),
          configManager,
          dbConnector,
          tablesConfig
      );
    }

    TestCoordinatorConfigManager(
        JacksonConfigManager jackson,
        ConfigManager configManager,
        TestDBConnector dbConnector,
        MetadataStorageTablesConfig tablesConfig
    )
    {
      super(jackson, dbConnector, tablesConfig);
      this.delegate = configManager;
    }

    @Override
    public ConfigManager.SetResult getAndUpdateCompactionConfig(
        UnaryOperator<DruidCompactionConfig> operator,
        AuditInfo auditInfo
    )
    {
      ++numUpdateAttempts;
      if (configUpdateResult == null) {
        return super.getAndUpdateCompactionConfig(operator, auditInfo);
      } else {
        return configUpdateResult;
      }
    }
  }

  /**
   * Test implementation for in-memory insert, lookup and compareAndSwap operations.
   */
  private static class TestDBConnector extends TestMetadataStorageConnector
  {
    private final Map<List<String>, byte[]> values = new HashMap<>();

    @Override
    public Void insertOrUpdate(String tableName, String keyColumn, String valueColumn, String key, byte[] value)
    {
      values.put(
          Arrays.asList(tableName, keyColumn, valueColumn, key),
          value
      );
      return null;
    }

    @Nullable
    @Override
    public byte[] lookup(String tableName, String keyColumn, String valueColumn, String key)
    {
      return values.get(Arrays.asList(tableName, keyColumn, valueColumn, key));
    }

    @Override
    public boolean compareAndSwap(List<MetadataCASUpdate> updates)
    {
      for (MetadataCASUpdate update : updates) {
        final List<String> key = Arrays.asList(
            update.getTableName(),
            update.getKeyColumn(),
            update.getValueColumn(),
            update.getKey()
        );

        final byte[] currentValue = values.get(key);
        if (currentValue == null || Arrays.equals(currentValue, update.getOldValue())) {
          values.put(key, update.getNewValue());
        } else {
          return false;
        }
      }

      return true;
    }
  }
}
