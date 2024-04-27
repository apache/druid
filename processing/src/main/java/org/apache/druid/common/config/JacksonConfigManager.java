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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class JacksonConfigManager
{
  private final ConfigManager configManager;
  private final ObjectMapper jsonMapper;
  private final AuditManager auditManager;

  @Inject
  public JacksonConfigManager(
      ConfigManager configManager,
      @Json ObjectMapper jsonMapper,
      AuditManager auditManager
  )
  {
    this.configManager = configManager;
    this.jsonMapper = jsonMapper;
    this.auditManager = auditManager;
  }

  public <T> AtomicReference<T> watch(String key, Class<? extends T> clazz)
  {
    return watch(key, clazz, null);
  }

  public <T> AtomicReference<T> watch(String key, Class<? extends T> clazz, T defaultVal)
  {
    return configManager.watchConfig(key, create(clazz, defaultVal));
  }

  public <T> AtomicReference<T> watch(String key, TypeReference<T> clazz, T defaultVal)
  {
    return configManager.watchConfig(key, create(clazz, defaultVal));
  }

  public <T> T convertByteToConfig(byte[] configInByte, Class<? extends T> clazz, T defaultVal)
  {
    if (configInByte == null) {
      return defaultVal;
    } else {
      final ConfigSerde<T> serde = create(clazz, defaultVal);
      return serde.deserialize(configInByte);
    }
  }

  /**
   * Set the config and add audit entry
   *
   * @param key of the config to set
   * @param val new config value to insert
   * @param auditInfo metadata regarding the change to config, for audit purposes
   */
  public <T> SetResult set(String key, T val, AuditInfo auditInfo)
  {
    return set(key, null, val, auditInfo);
  }

  /**
   * Set the config and add audit entry
   *
   * @param key of the config to set
   * @param oldValue old config value. If not null, then the update will only succeed if the insert
   *                 happens when current database entry is the same as this value. Note that the current database
   *                 entry (in array of bytes) have to be exactly the same as the array of bytes of this value for
   *                 update to succeed. If null, then the insert will not consider the current database entry. Note
   *                 that this field intentionally uses byte array to be resilient across serde of existing data
   *                 retrieved from the database (instead of Java object which may have additional fields added
   *                 as a result of serde)
   * @param newValue new config value to insert
   * @param auditInfo metadata regarding the change to config, for audit purposes
   */
  public <T> SetResult set(
      String key,
      @Nullable byte[] oldValue,
      T newValue,
      AuditInfo auditInfo
  )
  {
    ConfigSerde configSerde = create(newValue.getClass(), null);
    // Audit and actual config change are done in separate transactions
    // there can be phantom audits and reOrdering in audit changes as well.
    auditManager.doAudit(
        AuditEntry.builder()
                  .key(key)
                  .type(key)
                  .auditInfo(auditInfo)
                  .payload(newValue)
                  .build()
    );
    return configManager.set(key, configSerde, oldValue, newValue);
  }

  @VisibleForTesting
  <T> ConfigSerde<T> create(final Class<? extends T> clazz, final T defaultVal)
  {
    return new ConfigSerde<T>()
    {
      @Override
      public byte[] serialize(T obj)
      {
        try {
          return jsonMapper.writeValueAsBytes(obj);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T deserialize(byte[] bytes)
      {
        if (bytes == null) {
          return defaultVal;
        }

        return JacksonUtils.readValue(jsonMapper, bytes, clazz);
      }
    };
  }

  @VisibleForTesting
  <T> ConfigSerde<T> create(final TypeReference<? extends T> clazz, final T defaultVal)
  {
    return new ConfigSerde<T>()
    {
      @Override
      public byte[] serialize(T obj)
      {
        try {
          return jsonMapper.writeValueAsBytes(obj);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T deserialize(byte[] bytes)
      {
        if (bytes == null) {
          return defaultVal;
        }

        try {
          return jsonMapper.readValue(bytes, clazz);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
