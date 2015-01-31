/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.common.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class JacksonConfigManager
{
  private final ConfigManager configManager;
  private final ObjectMapper jsonMapper;

  @Inject
  public JacksonConfigManager(
      ConfigManager configManager,
      ObjectMapper jsonMapper
  )
  {
    this.configManager = configManager;
    this.jsonMapper = jsonMapper;
  }

  public <T> AtomicReference<T> watch(String key, Class<? extends T> clazz)
  {
    return watch(key, clazz, null);
  }

  public <T> AtomicReference<T> watch(String key, Class<? extends T> clazz, T defaultVal)
  {
    return configManager.watchConfig(key, create(clazz, defaultVal));
  }

  public <T> AtomicReference<T> watch(String key, TypeReference<T> clazz)
  {
    return watch(key, clazz, null);
  }

  public <T> AtomicReference<T> watch(String key, TypeReference<T> clazz, T defaultVal)
  {
    return configManager.watchConfig(key, create(clazz, defaultVal));
  }

  public <T> boolean set(String key, T val)
  {
    return configManager.set(key, create(val.getClass(), null), val);
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
          throw Throwables.propagate(e);
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
          throw Throwables.propagate(e);
        }
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
          throw Throwables.propagate(e);
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
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
