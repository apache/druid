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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Objects;

@RunWith(MockitoJUnitRunner.class)
public class JacksonConfigManagerTest
{
  @Mock
  private ConfigManager mockConfigManager;

  @Mock
  private AuditManager mockAuditManager;

  private JacksonConfigManager jacksonConfigManager;

  @Before
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
    Assert.assertNotNull(auditCapture.getValue());
  }

  @Test
  public void testConvertByteToConfigWithNullConfigInByte()
  {
    TestConfig defaultExpected = new TestConfig("version", null, 3);
    TestConfig actual = jacksonConfigManager.convertByteToConfig(null, TestConfig.class, defaultExpected);
    Assert.assertEquals(defaultExpected, actual);
  }

  @Test
  public void testConvertByteToConfigWithNonNullConfigInByte()
  {
    ConfigSerde<TestConfig> configConfigSerdeFromTypeReference = jacksonConfigManager.create(new TypeReference<TestConfig>()
    {
    }, null);
    TestConfig defaultConfig = new TestConfig("version", null, 3);
    TestConfig expectedConfig = new TestConfig("version2", null, 5);
    byte[] expectedConfigInByte = configConfigSerdeFromTypeReference.serialize(expectedConfig);

    TestConfig actual = jacksonConfigManager.convertByteToConfig(expectedConfigInByte, TestConfig.class, defaultConfig);
    Assert.assertEquals(expectedConfig, actual);
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
