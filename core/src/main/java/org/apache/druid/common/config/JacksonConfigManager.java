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
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

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
      ObjectMapper jsonMapper,
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

  public <T> SetResult set(String key, T val, AuditInfo auditInfo)
  {
    ConfigSerde configSerde = create(val.getClass(), null);
    // Audit and actual config change are done in separate transactions
    // there can be phantom audits and reOrdering in audit changes as well.
    auditManager.doAudit(
        AuditEntry.builder()
                  .key(key)
                  .type(key)
                  .auditInfo(auditInfo)
                  .payload(configSerde.serializeToString(val))
                  .build()
    );
    return configManager.set(key, configSerde, val);
  }

  private <T> ConfigSerde<T> create(final Class<? extends T> clazz, final T defaultVal)
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
      public String serializeToString(T obj)
      {
        try {
          return jsonMapper.writeValueAsString(obj);
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

  private <T> ConfigSerde<T> create(final TypeReference<? extends T> clazz, final T defaultVal)
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
      public String serializeToString(T obj)
      {
        try {
          return jsonMapper.writeValueAsString(obj);
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
