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
import java.util.function.UnaryOperator;

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

  @Nullable
  public byte[] getCurrentBytes(String key)
  {
    return configManager.getCurrentBytes(key);
  }

  public boolean isCompareAndSwapEnabled()
  {
    return configManager.isCompareAndSwapEnabled();
  }

  /**
   * Set the config, optionally guarded by an {@code If-Match}-style
   * precondition. When {@code ifMatchEtag} is {@code null}, behaves like
   * {@link #set(String, Object, AuditInfo)}. Otherwise the write only commits
   * if the currently stored bytes hash to {@code ifMatchEtag}; on mismatch the
   * result reports {@link SetResult#isPreconditionFailed() preconditionFailed}.
   *
   * <p>The precondition is enforced via metadata-store CAS, so conditional
   * writes require {@code druid.manager.config.enableCompareAndSwap} to be
   * true (the default). With CAS disabled, {@code If-Match} writes fail as a
   * precondition failure instead of silently degrading to last-writer-wins.
   */
  public <T> SetResult setIfMatch(
      String key,
      @Nullable String ifMatchEtag,
      T newValue,
      AuditInfo auditInfo
  )
  {
    if (newValue == null) {
      return SetResult.failure(new IllegalArgumentException("input obj is null"));
    }
    if (ifMatchEtag == null) {
      return set(key, newValue, auditInfo);
    }
    if (!configManager.isCompareAndSwapEnabled()) {
      return SetResult.preconditionFailed(
          new IllegalStateException(
              "If-Match requires druid.manager.config.enableCompareAndSwap to be enabled for key[" + key + "]"
          )
      );
    }
    final byte[] currentBytes = configManager.getCurrentBytes(key);
    if (!ConfigEtag.matches(ifMatchEtag, currentBytes)) {
      return SetResult.preconditionFailed(
          new IllegalStateException("If-Match precondition failed for key[" + key + "]")
      );
    }
    return casConflictAsPreconditionFailed(set(key, currentBytes, newValue, auditInfo), key);
  }

  /**
   * Applies {@code updateOperator} to the config deserialized from the bytes
   * used for {@code If-Match} validation and CAS. This is intended for partial
   * updates whose output must be built from the same snapshot that the
   * precondition protects.
   */
  public <T> SetResult setIfMatch(
      String key,
      @Nullable String ifMatchEtag,
      Class<? extends T> clazz,
      @Nullable T defaultVal,
      UnaryOperator<T> updateOperator,
      AuditInfo auditInfo
  )
  {
    if (ifMatchEtag != null && !configManager.isCompareAndSwapEnabled()) {
      return SetResult.preconditionFailed(
          new IllegalStateException(
              "If-Match requires druid.manager.config.enableCompareAndSwap to be enabled for key[" + key + "]"
          )
      );
    }
    final byte[] currentBytes = configManager.getCurrentBytes(key);
    if (ifMatchEtag != null && !ConfigEtag.matches(ifMatchEtag, currentBytes)) {
      return SetResult.preconditionFailed(
          new IllegalStateException("If-Match precondition failed for key[" + key + "]")
      );
    }
    final ConfigSerde<T> configSerde = create(clazz, defaultVal);
    final T currentValue = configSerde.deserialize(currentBytes);
    final T newValue = updateOperator.apply(currentValue);
    if (newValue == null) {
      return SetResult.failure(new IllegalArgumentException("input obj is null"));
    }
    if (ifMatchEtag == null) {
      return set(key, newValue, auditInfo);
    }
    return casConflictAsPreconditionFailed(set(key, currentBytes, newValue, auditInfo), key);
  }

  /**
   * Maps a CAS-conflict ({@link SetResult#isRetryable() retryable}) outcome to a
   * precondition failure. A conditional write that loses the CAS means another
   * writer committed between our read and write, so the supplied {@code If-Match}
   * no longer describes the stored value — the caller must re-read and retry.
   */
  private static SetResult casConflictAsPreconditionFailed(SetResult result, String key)
  {
    if (result.isRetryable()) {
      return SetResult.preconditionFailed(
          new IllegalStateException("If-Match precondition failed (concurrent update) for key[" + key + "]")
      );
    }
    return result;
  }

  @VisibleForTesting
  <T> ConfigSerde<T> create(final Class<? extends T> clazz, final T defaultVal)
  {
    return new ConfigSerde<>()
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
    return new ConfigSerde<>()
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
