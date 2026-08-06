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
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfigAuditEntry;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class CoordinatorCompactionConfigsResourceTest
{
  private static final double DELTA = 1e-9;
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Mock
  private HttpServletRequest mockHttpServletRequest;

  private TestCoordinatorConfigManager configManager;
  private CoordinatorCompactionConfigsResource resource;

  @BeforeEach
  public void setup()
  {
    Mockito.when(mockHttpServletRequest.getRemoteAddr()).thenReturn("123");
    final AuditManager auditManager = new TestAuditManager();
    configManager = TestCoordinatorConfigManager.create(auditManager);
    resource = new CoordinatorCompactionConfigsResource(configManager);
    configManager.delegate.start();
  }

  @AfterEach
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

    Assertions.assertEquals(0.1, defaultConfig.getCompactionTaskSlotRatio(), DELTA);
    Assertions.assertEquals(Integer.MAX_VALUE, defaultConfig.getMaxCompactionTaskSlots());
    Assertions.assertTrue(defaultConfig.getCompactionConfigs().isEmpty());
    Assertions.assertTrue(defaultConfig.isUseSupervisors());
    Assertions.assertEquals(CompactionEngine.NATIVE, defaultConfig.getEngine());
  }

  @Test
  public void testSetCompactionTaskLimit()
  {
    resource.setCompactionTaskLimit(0.1, 100, mockHttpServletRequest);

    final DruidCompactionConfig oldConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);
    Assertions.assertEquals(100, oldConfig.getMaxCompactionTaskSlots());
    Assertions.assertEquals(0.1, oldConfig.getCompactionTaskSlotRatio(), 1e-9);

    Response response = resource.setCompactionTaskLimit(0.5, 9, mockHttpServletRequest);
    verifyStatus(Response.Status.OK, response);

    final DruidCompactionConfig updatedConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);

    // Verify that the task slot fields have been updated
    Assertions.assertEquals(0.5, updatedConfig.getCompactionTaskSlotRatio(), DELTA);
    Assertions.assertEquals(9, updatedConfig.getMaxCompactionTaskSlots());

    // Verify that other cluster config fields are unchanged
    Assertions.assertEquals(oldConfig.isUseSupervisors(), updatedConfig.isUseSupervisors());
    Assertions.assertEquals(oldConfig.getCompactionPolicy(), updatedConfig.getCompactionPolicy());
    Assertions.assertEquals(oldConfig.getEngine(), updatedConfig.getEngine());

    // Verify that the other fields are unchanged
    Assertions.assertEquals(oldConfig.getCompactionConfigs(), updatedConfig.getCompactionConfigs());
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
        = InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();
    Response response = resource.addOrUpdateDatasourceCompactionConfig(newDatasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.OK, response);

    final DataSourceCompactionConfig fetchedDatasourceConfig
        = verifyAndGetPayload(resource.getDatasourceCompactionConfig(TestDataSource.WIKI), InlineSchemaDataSourceCompactionConfig.class);
    Assertions.assertEquals(newDatasourceConfig, fetchedDatasourceConfig);

    final DruidCompactionConfig fullCompactionConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);
    Assertions.assertEquals(1, fullCompactionConfig.getCompactionConfigs().size());
    Assertions.assertEquals(newDatasourceConfig, fullCompactionConfig.getCompactionConfigs().get(0));
  }

  @Test
  public void testAddDatasourceConfigWithMSQEngineIsInvalid()
  {
    final DataSourceCompactionConfig newDatasourceConfig
        = InlineSchemaDataSourceCompactionConfig.builder()
                                                .forDataSource(TestDataSource.WIKI)
                                                .withEngine(CompactionEngine.MSQ)
                                                .build();
    Response response = resource.addOrUpdateDatasourceCompactionConfig(newDatasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.BAD_REQUEST, response);
    Assertions.assertTrue(response.getEntity() instanceof ErrorResponse);
    Assertions.assertEquals(
        "MSQ engine is supported only with supervisor-based compaction on the Overlord.",
        ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage()
    );
  }

  @Test
  public void testUpdateDatasourceConfig()
  {
    final DataSourceCompactionConfig originalDatasourceConfig = InlineSchemaDataSourceCompactionConfig
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

    final DataSourceCompactionConfig updatedDatasourceConfig = InlineSchemaDataSourceCompactionConfig
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
        = verifyAndGetPayload(resource.getDatasourceCompactionConfig(TestDataSource.WIKI), InlineSchemaDataSourceCompactionConfig.class);
    Assertions.assertEquals(originalDatasourceConfig, latestDatasourceConfig);

    final DruidCompactionConfig fullCompactionConfig
        = verifyAndGetPayload(resource.getCompactionConfig(), DruidCompactionConfig.class);
    Assertions.assertEquals(1, fullCompactionConfig.getCompactionConfigs().size());
    Assertions.assertEquals(originalDatasourceConfig, fullCompactionConfig.getCompactionConfigs().get(0));
  }

  @Test
  public void testDeleteDatasourceConfig()
  {
    final DataSourceCompactionConfig datasourceConfig
        = InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();
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
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        mockHttpServletRequest
    );

    Assertions.assertEquals(
        5,
        configManager.numUpdateAttempts
    );
  }

  @Test
  public void testUpdateIsNotRetriedIfFailureIsNotRetryable()
  {
    configManager.configUpdateResult
        = ConfigManager.SetResult.failure(new Exception("not retryable"));
    resource.addOrUpdateDatasourceCompactionConfig(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        mockHttpServletRequest
    );

    Assertions.assertEquals(1, configManager.numUpdateAttempts);
  }

  @Test
  public void testGetDatasourceConfigHistory()
  {
    final InlineSchemaDataSourceCompactionConfig.Builder builder
        = InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI);

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
    Assertions.assertEquals(3, history.size());
    Assertions.assertEquals(configV1, history.get(0).getCompactionConfig());
    Assertions.assertEquals(configV2, history.get(1).getCompactionConfig());
    Assertions.assertEquals(configV3, history.get(2).getCompactionConfig());
  }

  @Test
  public void testGetHistoryOfUnknownDatasourceReturnsEmpty()
  {
    Response response = resource.getCompactionConfigHistory(TestDataSource.WIKI, null, null);
    verifyStatus(Response.Status.OK, response);
    Assertions.assertTrue(((List<?>) response.getEntity()).isEmpty());
  }

  @Test
  public void testAddInvalidDatasourceConfigThrowsBadRequest()
  {
    final DataSourceCompactionConfig datasourceConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTaskContext(Collections.singletonMap(ClientMSQContext.CTX_MAX_NUM_TASKS, 1))
        .withEngine(CompactionEngine.MSQ)
        .build();

    final Response response = resource.addOrUpdateDatasourceCompactionConfig(datasourceConfig, mockHttpServletRequest);
    verifyStatus(Response.Status.BAD_REQUEST, response);
    Assertions.assertTrue(response.getEntity() instanceof ErrorResponse);
    Assertions.assertEquals(
        "MSQ engine is supported only with supervisor-based compaction on the Overlord.",
        ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage()
    );
  }
  @SuppressWarnings("unchecked")
  private <T> T verifyAndGetPayload(Response response, Class<T> type)
  {
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    Assertions.assertTrue(type.isInstance(response.getEntity()));
    return (T) response.getEntity();
  }

  private void verifyStatus(Response.Status expectedStatus, Response response)
  {
    Assertions.assertEquals(expectedStatus.getStatusCode(), response.getStatus());
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
          auditManager,
          dbConnector,
          tablesConfig
      );
    }

    TestCoordinatorConfigManager(
        JacksonConfigManager jackson,
        ConfigManager configManager,
        AuditManager auditManager,
        TestDBConnector dbConnector,
        MetadataStorageTablesConfig tablesConfig
    )
    {
      super(jackson, dbConnector, tablesConfig, auditManager);
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
