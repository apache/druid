/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
