/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli.convert;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;

import java.util.Map;
import java.util.Properties;

/**
 */
public class IndexCacheConverter implements PropertyConverter
{

  private static final String PROPERTY = "druid.paths.indexCache";

  @Override
  public boolean canHandle(String property)
  {
    return PROPERTY.equals(property);
  }

  @Override
  public Map<String, String> convert(Properties properties)
  {
    final String value = properties.getProperty(PROPERTY);

    return ImmutableMap.of(
        "druid.segmentCache.locations",
        StringUtils.format(
            "[{\"path\": \"%s\", \"maxSize\": %s}]", value, properties.getProperty("druid.server.maxSize")
        )
    );
  }
}
