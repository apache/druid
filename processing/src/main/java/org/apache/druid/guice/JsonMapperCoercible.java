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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.logger.Logger;
import org.skife.config.Coercer;
import org.skife.config.Coercible;

import java.io.IOException;

/**
 *
 */
public class JsonMapperCoercible<T> implements Coercible<T>
{
  private static final Logger log = new Logger(JsonMapperCoercible.class);
  private final ObjectMapper mapper;
  private final Class<T> clazz;
  private final T defaultVal;

  public JsonMapperCoercible(ObjectMapper mapper, Class<T> clazz, T defaultVal)
  {
    this.mapper = mapper;
    this.clazz = clazz;
    this.defaultVal = defaultVal;
  }

  @Override
  public Coercer<T> accept(Class<?> clazz)
  {
    if (this.clazz != clazz) {
      return null;
    }
    return new Coercer<T>()
    {
      @Override
      public T coerce(String value)
      {
        if (Strings.isNullOrEmpty(value)) {
          return defaultVal;
        }
        try {
          return mapper.readValue(value, JsonMapperCoercible.this.clazz);
        }
        catch (IOException e) {
          log.warn(e, "coerce %s error,will use default value", JsonMapperCoercible.this.clazz.getSimpleName());
        }
        return defaultVal;
      }
    };
  }
}

