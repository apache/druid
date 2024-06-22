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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestMetadataStorageConnector;
import org.apache.druid.metadata.TestMetadataStorageTablesConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigManagerTest
{
  private static final String CONFIG_KEY = "configX";
  private static final String TABLE_NAME = "config_table";
  private static final byte[] OLD_CONFIG = {1, 2, 3};
  private static final TestConfig NEW_CONFIG = new TestConfig("2", "y", 2);

  private MetadataStorageConnector dbConnector;
  private MetadataStorageTablesConfig metadataStorageTablesConfig;
  private TestConfigManagerConfig configManagerConfig;

  private ConfigSerde<TestConfig> configConfigSerdeFromClass;
  private ConfigManager configManager;
  private JacksonConfigManager jacksonConfigManager;

  public void setup()
  {
    setup(new TestMetadataStorageConnector());
  }

  public void setup(TestMetadataStorageConnector dbConnector)
  {
    this.dbConnector = dbConnector;
    metadataStorageTablesConfig = new TestMetadataStorageTablesConfig()
    {
      @Override
      public String getConfigTable()
      {
        return TABLE_NAME;
      }
    };
    configManagerConfig = new TestConfigManagerConfig();
    configManager = new ConfigManager(
        this.dbConnector,
        Suppliers.ofInstance(metadataStorageTablesConfig),
        Suppliers.ofInstance(configManagerConfig)
    );
    jacksonConfigManager = new JacksonConfigManager(
        configManager,
        new ObjectMapper(),
        null
    );
    configConfigSerdeFromClass = jacksonConfigManager.create(TestConfig.class, null);
  }

  @Test
  public void testSetNewObjectIsNull()
  {
    setup();
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, null);
    Assert.assertFalse(setResult.isOk());
    Assert.assertFalse(setResult.isRetryable());
    Assert.assertTrue(setResult.getException() instanceof IllegalAccessException);
  }

  @Test
  public void testSetConfigManagerNotStarted()
  {
    setup();
    ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, NEW_CONFIG);
    Assert.assertFalse(setResult.isOk());
    Assert.assertFalse(setResult.isRetryable());
    Assert.assertTrue(setResult.getException() instanceof IllegalStateException);
  }

  @Test
  public void testSetOldObjectNullShouldInsertWithoutSwap()
  {
    final AtomicBoolean called = new AtomicBoolean();
    setup(new TestMetadataStorageConnector()
    {

      @Override
      public Void insertOrUpdate(String tableName, String keyColumn, String valueColumn, String key, byte[] value)
      {
        Assert.assertFalse(called.getAndSet(true));
        Assert.assertEquals(TABLE_NAME, tableName);
        Assert.assertEquals(CONFIG_KEY, key);
        return null;
      }
    });

    try {
      configManager.start();
      ConfigManager.SetResult setResult = configManager.set(CONFIG_KEY, configConfigSerdeFromClass, null, NEW_CONFIG);
      Assert.assertTrue(setResult.isOk());
      Assert.assertTrue(called.get());
    }
    finally {
      configManager.stop();
    }
  }

  @Test
  public void testSetOldObjectNotNullShouldSwap()
  {
    final AtomicBoolean called = new AtomicBoolean();
    setup(new TestMetadataStorageConnector()
    {
      @Override
      public boolean compareAndSwap(List<MetadataCASUpdate> updates)
      {
        Assert.assertFalse(called.getAndSet(true));
        Assert.assertEquals(1, updates.size());
        final MetadataCASUpdate singularUpdate = updates.get(0);
        Assert.assertEquals(TABLE_NAME, singularUpdate.getTableName());
        Assert.assertEquals(MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN, singularUpdate.getKeyColumn());
        Assert.assertEquals(MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN, singularUpdate.getValueColumn());
        Assert.assertEquals(CONFIG_KEY, singularUpdate.getKey());
        Assert.assertArrayEquals(OLD_CONFIG, singularUpdate.getOldValue());
        Assert.assertArrayEquals(configConfigSerdeFromClass.serialize(NEW_CONFIG), singularUpdate.getNewValue());
        return true;
      }
    });
    try {
      configManager.start();
      ConfigManager.SetResult setResult = configManager.set(
          CONFIG_KEY,
          configConfigSerdeFromClass,
          OLD_CONFIG,
          NEW_CONFIG
      );
      Assert.assertTrue(setResult.isOk());
      Assert.assertTrue(called.get());
    }
    finally {
      configManager.stop();
    }
  }

  @Test
  public void testSetOldObjectNotNullButCompareAndSwapDisabledShouldInsertWithoutSwap()
  {
    final AtomicBoolean called = new AtomicBoolean();

    setup(new TestMetadataStorageConnector()
    {
      @Override
      public Void insertOrUpdate(String tableName, String keyColumn, String valueColumn, String key, byte[] value)
      {
        Assert.assertFalse(called.getAndSet(true));
        Assert.assertEquals(TABLE_NAME, tableName);
        Assert.assertEquals(CONFIG_KEY, key);
        return null;
      }
    });
    configManagerConfig.enableCompareAndSwap = false;

    try {
      configManager.start();
      ConfigManager.SetResult setResult = configManager.set(
          CONFIG_KEY,
          configConfigSerdeFromClass,
          OLD_CONFIG,
          NEW_CONFIG
      );
      Assert.assertTrue(setResult.isOk());
      Assert.assertTrue(called.get());
    }
    finally {
      configManager.stop();
    }
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
