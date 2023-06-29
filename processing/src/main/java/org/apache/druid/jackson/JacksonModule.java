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

package org.apache.druid.jackson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.JsonNonNull;
import org.apache.druid.guice.annotations.Smile;

import javax.annotation.Nullable;

import java.util.Properties;

/**
 */
public class JacksonModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
  }

  @Provides @LazySingleton @Json
  public ObjectMapper jsonMapper(Properties props)
  {
    return new DefaultObjectMapper(getServiceName(props));
  }

  /**
   * Provides ObjectMapper that suppress serializing properties with null values
   */
  @Provides @LazySingleton @JsonNonNull
  public ObjectMapper jsonMapperOnlyNonNullValue(Properties props)
  {
    return new DefaultObjectMapper(getServiceName(props)).setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  @Provides @LazySingleton @Smile
  public ObjectMapper smileMapper(Properties props)
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory, getServiceName(props));
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }

  @Nullable
  private String getServiceName(Properties properties)
  {
    if (null == properties) {
      return null;
    }
    return properties.getProperty("druid.service");
  }
}
