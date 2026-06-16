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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@ExtendWith(MockitoExtension.class)
public class JacksonConfigManagerTest
{
  @Mock
  private ConfigManager mockConfigManager;

  @Mock
  private AuditManager mockAuditManager;

  private JacksonConfigManager jacksonConfigManager;

  @BeforeEach
  public void setUp()
  {
    jacksonConfigManager = new JacksonConfigManager(
        mockConfigManager,
        new ObjectMapper(),
        mockAuditManager
    );
  }

  @Test
  public void testSet()
  {
    String key = "key";
    TestConfig val = new TestConfig("version", "string", 3);
    AuditInfo auditInfo = new AuditInfo(
        "testAuthor",
        "testIdentity",
        "testComment",
        "127.0.0.1"
    );

    jacksonConfigManager.set(key, val, auditInfo);

    ArgumentCaptor<AuditEntry> auditCapture = ArgumentCaptor.forClass(AuditEntry.class);
    Mockito.verify(mockAuditManager).doAudit(auditCapture.capture());
    Assertions.assertNotNull(auditCapture.getValue());
  }

  @Test
  public void testSetIfMatchNullEtagDelegatesToUnconditionalSet()
  {
    String key = "key";
    TestConfig val = new TestConfig("v", "s", 1);
    AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    Mockito.when(mockConfigManager.set(
        Mockito.eq(key),
        Mockito.any(ConfigSerde.class),
        Mockito.isNull(),
        Mockito.eq(val)
    )).thenReturn(ConfigManager.SetResult.ok());

    ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(key, null, val, auditInfo);

    Assertions.assertTrue(result.isOk());
    Mockito.verify(mockConfigManager).set(
        Mockito.eq(key),
        Mockito.any(ConfigSerde.class),
        Mockito.isNull(),
        Mockito.eq(val)
    );
  }

  @Test
  public void testSetIfMatchPreconditionPassesForMatchingEtag()
  {
    String key = "key";
    TestConfig val = new TestConfig("v", "s", 1);
    AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    byte[] currentBytes = "current".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    Mockito.when(mockConfigManager.isCompareAndSwapEnabled()).thenReturn(true);
    Mockito.when(mockConfigManager.getCurrentBytes(key)).thenReturn(currentBytes);
    Mockito.when(mockConfigManager.set(
        Mockito.eq(key),
        Mockito.any(ConfigSerde.class),
        Mockito.eq(currentBytes),
        Mockito.eq(val)
    )).thenReturn(ConfigManager.SetResult.ok());

    String etag = ConfigEtag.compute(currentBytes);
    ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(key, etag, val, auditInfo);

    Assertions.assertTrue(result.isOk());
    Assertions.assertFalse(result.isPreconditionFailed());
    Mockito.verify(mockConfigManager).set(
        Mockito.eq(key),
        Mockito.any(ConfigSerde.class),
        Mockito.eq(currentBytes),
        Mockito.eq(val)
    );
  }

  @Test
  public void testSetIfMatchPreconditionFailsForMismatchedEtag()
  {
    String key = "key";
    TestConfig val = new TestConfig("v", "s", 1);
    AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    Mockito.when(mockConfigManager.isCompareAndSwapEnabled()).thenReturn(true);
    Mockito.when(mockConfigManager.getCurrentBytes(key)).thenReturn("current".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(
        key,
        ConfigEtag.compute("stale".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        val,
        auditInfo
    );

    Assertions.assertFalse(result.isOk());
    Assertions.assertTrue(result.isPreconditionFailed());
    Mockito.verify(mockConfigManager, Mockito.never()).set(
        Mockito.anyString(),
        Mockito.any(ConfigSerde.class),
        Mockito.any(byte[].class),
        Mockito.any()
    );
  }

  @Test
  public void testSetIfMatchPreconditionFailsWhenCompareAndSwapDisabled()
  {
    final String key = "key";
    final TestConfig val = new TestConfig("v", "s", 1);
    final AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    Mockito.when(mockConfigManager.isCompareAndSwapEnabled()).thenReturn(false);

    final ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(key, "\"etag\"", val, auditInfo);

    Assertions.assertFalse(result.isOk());
    Assertions.assertTrue(result.isPreconditionFailed());
    Mockito.verify(mockConfigManager, Mockito.never()).getCurrentBytes(Mockito.anyString());
    Mockito.verify(mockConfigManager, Mockito.never()).set(
        Mockito.anyString(),
        Mockito.any(ConfigSerde.class),
        Mockito.any(byte[].class),
        Mockito.any()
    );
    Mockito.verify(mockAuditManager, Mockito.never()).doAudit(Mockito.any(AuditEntry.class));
  }

  @Test
  public void testSetIfMatchTransformBuildsNewValueFromMatchedBytes()
  {
    final String key = "key";
    final AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    final TestConfig current = new TestConfig("v1", "current", 1);
    final TestConfig updated = new TestConfig("v1", "updated", 2);
    final ConfigSerde<TestConfig> serde = jacksonConfigManager.create(TestConfig.class, null);
    final byte[] currentBytes = serde.serialize(current);

    Mockito.when(mockConfigManager.isCompareAndSwapEnabled()).thenReturn(true);
    Mockito.when(mockConfigManager.getCurrentBytes(key)).thenReturn(currentBytes);
    Mockito.when(mockConfigManager.set(
        Mockito.eq(key),
        Mockito.any(ConfigSerde.class),
        Mockito.eq(currentBytes),
        Mockito.eq(updated)
    )).thenReturn(ConfigManager.SetResult.ok());

    final ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(
        key,
        ConfigEtag.compute(currentBytes),
        TestConfig.class,
        null,
        currentValue -> new TestConfig(currentValue.getVersion(), "updated", currentValue.getSettingInt() + 1),
        auditInfo
    );

    Assertions.assertTrue(result.isOk());
    Mockito.verify(mockConfigManager).set(
        Mockito.eq(key),
        Mockito.any(ConfigSerde.class),
        Mockito.eq(currentBytes),
        Mockito.eq(updated)
    );
  }

  @Test
  public void testSetIfMatchTransformDoesNotApplyUpdateForMismatchedEtag()
  {
    final String key = "key";
    final AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    final TestConfig current = new TestConfig("v1", "current", 1);
    final ConfigSerde<TestConfig> serde = jacksonConfigManager.create(TestConfig.class, null);
    final byte[] currentBytes = serde.serialize(current);
    final AtomicBoolean updateCalled = new AtomicBoolean(false);

    Mockito.when(mockConfigManager.isCompareAndSwapEnabled()).thenReturn(true);
    Mockito.when(mockConfigManager.getCurrentBytes(key)).thenReturn(currentBytes);

    final ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(
        key,
        ConfigEtag.compute("stale".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        TestConfig.class,
        null,
        currentValue -> {
          updateCalled.set(true);
          return currentValue;
        },
        auditInfo
    );

    Assertions.assertFalse(result.isOk());
    Assertions.assertTrue(result.isPreconditionFailed());
    Assertions.assertFalse(updateCalled.get());
    Mockito.verify(mockConfigManager, Mockito.never()).set(
        Mockito.anyString(),
        Mockito.any(ConfigSerde.class),
        Mockito.any(byte[].class),
        Mockito.any()
    );
    Mockito.verify(mockAuditManager, Mockito.never()).doAudit(Mockito.any(AuditEntry.class));
  }

  @Test
  public void testSetIfMatchTransformPreconditionFailsWhenCompareAndSwapDisabled()
  {
    final String key = "key";
    final AuditInfo auditInfo = new AuditInfo("a", "i", "c", "ip");
    final AtomicBoolean updateCalled = new AtomicBoolean(false);
    Mockito.when(mockConfigManager.isCompareAndSwapEnabled()).thenReturn(false);

    final ConfigManager.SetResult result = jacksonConfigManager.setIfMatch(
        key,
        "\"etag\"",
        TestConfig.class,
        null,
        currentValue -> {
          updateCalled.set(true);
          return currentValue;
        },
        auditInfo
    );

    Assertions.assertFalse(result.isOk());
    Assertions.assertTrue(result.isPreconditionFailed());
    Assertions.assertFalse(updateCalled.get());
    Mockito.verify(mockConfigManager, Mockito.never()).getCurrentBytes(Mockito.anyString());
    Mockito.verify(mockConfigManager, Mockito.never()).set(
        Mockito.anyString(),
        Mockito.any(ConfigSerde.class),
        Mockito.any(byte[].class),
        Mockito.any()
    );
    Mockito.verify(mockAuditManager, Mockito.never()).doAudit(Mockito.any(AuditEntry.class));
  }

  @Test
  public void testConvertByteToConfigWithNullConfigInByte()
  {
    TestConfig defaultExpected = new TestConfig("version", null, 3);
    TestConfig actual = jacksonConfigManager.convertByteToConfig(null, TestConfig.class, defaultExpected);
    Assertions.assertEquals(defaultExpected, actual);
  }

  @Test
  public void testConvertByteToConfigWithNonNullConfigInByte()
  {
    ConfigSerde<TestConfig> configConfigSerdeFromTypeReference = jacksonConfigManager.create(new TypeReference<>() {}, null);
    TestConfig defaultConfig = new TestConfig("version", null, 3);
    TestConfig expectedConfig = new TestConfig("version2", null, 5);
    byte[] expectedConfigInByte = configConfigSerdeFromTypeReference.serialize(expectedConfig);

    TestConfig actual = jacksonConfigManager.convertByteToConfig(expectedConfigInByte, TestConfig.class, defaultConfig);
    Assertions.assertEquals(expectedConfig, actual);
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

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestConfig config = (TestConfig) o;
      return settingInt == config.settingInt &&
             Objects.equals(version, config.version) &&
             Objects.equals(settingString, config.settingString);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(version, settingString, settingInt);
    }
  }
}
