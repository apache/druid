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

package org.apache.druid.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
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

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConfigManagerTest
{
  private static final String CONFIG_KEY = "configX";
  private static final String TABLE_NAME = "config_table";
  private static final byte[] OLD_CONFIG = {1, 2, 3};
  private static final TestConfig NEW_CONFIG = new TestConfig("2", "y", 2);

  @Mock
  private MetadataStorageConnector mockDbConnector;

  @Mock
  private MetadataStorageTablesConfig mockMetadataStorageTablesConfig;

  @Mock
  private AuditManager mockAuditManager;

  @Mock
  private ConfigManagerConfig mockConfigManagerConfig;

  private ConfigSerde<TestConfig> configConfigSerdeFromClass;
  private ConfigManager configManager;
  private JacksonConfigManager jacksonConfigManager;

  @Before
  public void setup()
  {
    when(mockMetadataStorageTablesConfig.getConfigTable()).thenReturn(TABLE_NAME);
    when(mockConfigManagerConfig.getPollDuration()).thenReturn(new Period());
    configManager = new ConfigManager(mockDbConnector, Suppliers.ofInstance(mockMetadataStorageTablesConfig), Suppliers.ofInstance(mockConfigManagerConfig));
    jacksonConfigManager = new JacksonConfigManager(
        configManager,
        new ObjectMapper(),
        new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL),
        mockAuditManager
    );
    configConfigSerdeFromClass = jacksonConfigManager.create(TestConfig.class, null);
  }

  @Test
  public void testSetNewObjectIsNull()
  {
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, null);
    Assert.assertFalse(setResult.isOk());
    Assert.assertFalse(setResult.isRetryable());
    Assert.assertTrue(setResult.getException() instanceof IllegalAccessException);
  }

  @Test
  public void testSetConfigManagerNotStarted()
  {
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, NEW_CONFIG);
    Assert.assertFalse(setResult.isOk());
    Assert.assertFalse(setResult.isRetryable());
    Assert.assertTrue(setResult.getException() instanceof IllegalStateException);
  }

  @Test
  public void testSetOldObjectNullShouldInsertWithoutSwap()
  {
    configManager.start();
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, null, NEW_CONFIG);
    Assert.assertTrue(setResult.isOk());
    Mockito.verify(mockDbConnector).insertOrUpdate(
        ArgumentMatchers.eq(TABLE_NAME),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(CONFIG_KEY),
        ArgumentMatchers.any(byte[].class)
    );
    Mockito.verifyNoMoreInteractions(mockDbConnector);
  }

  @Test
  public void testSetOldObjectNotNullShouldSwap()
  {
    when(mockConfigManagerConfig.isEnableCompareAndSwap()).thenReturn(true);
    when(mockDbConnector.compareAndSwap(any(List.class))).thenReturn(true);
    final ArgumentCaptor<List<MetadataCASUpdate>> updateCaptor = ArgumentCaptor.forClass(List.class);
    configManager.start();
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, OLD_CONFIG, NEW_CONFIG);
    Assert.assertTrue(setResult.isOk());
    Mockito.verify(mockDbConnector).compareAndSwap(
        updateCaptor.capture()
    );
    Mockito.verifyNoMoreInteractions(mockDbConnector);
    Assert.assertEquals(1, updateCaptor.getValue().size());
    Assert.assertEquals(TABLE_NAME, updateCaptor.getValue().get(0).getTableName());
    Assert.assertEquals(MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN, updateCaptor.getValue().get(0).getKeyColumn());
    Assert.assertEquals(MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN, updateCaptor.getValue().get(0).getValueColumn());
    Assert.assertEquals(CONFIG_KEY, updateCaptor.getValue().get(0).getKey());
    Assert.assertArrayEquals(OLD_CONFIG, updateCaptor.getValue().get(0).getOldValue());
    Assert.assertArrayEquals(configConfigSerdeFromClass.serialize(NEW_CONFIG), updateCaptor.getValue().get(0).getNewValue());
  }

  @Test
  public void testSetOldObjectNotNullButCompareAndSwapDisabledShouldInsertWithoutSwap()
  {
    when(mockConfigManagerConfig.isEnableCompareAndSwap()).thenReturn(false);
    configManager.start();
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, OLD_CONFIG, NEW_CONFIG);
    Assert.assertTrue(setResult.isOk());
    Mockito.verify(mockDbConnector).insertOrUpdate(
        ArgumentMatchers.eq(TABLE_NAME),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(CONFIG_KEY),
        ArgumentMatchers.any(byte[].class)
    );
    Mockito.verifyNoMoreInteractions(mockDbConnector);
  }

  static class TestConfig
  {
    private final String version;
    private final String settingString;
    private final int settingInt;

    @JsonCreator
    public TestConfig(
        @JsonProperty("version") String version,
        @JsonProperty("settingString") String settingString,
        @JsonProperty("settingInt") int settingInt
    )
    {
      this.version = version;
      this.settingString = settingString;
      this.settingInt = settingInt;
    }

    public String getVersion()
    {
      return version;
    }

    public String getSettingString()
    {
      return settingString;
    }

    public int getSettingInt()
    {
      return settingInt;
    }
  }
}
