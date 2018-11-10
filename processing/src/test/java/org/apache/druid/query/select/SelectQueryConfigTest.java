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

package org.apache.druid.query.select;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class SelectQueryConfigTest
{
  private final ObjectMapper MAPPER = new DefaultObjectMapper();

  private final ImmutableMap<String, String> CONFIG_MAP = ImmutableMap
      .<String, String>builder()
      .put(SelectQueryConfig.ENABLE_FROM_NEXT_DEFAULT, "false")
      .build();

  private final ImmutableMap<String, String> CONFIG_MAP2 = ImmutableMap
      .<String, String>builder()
      .put(SelectQueryConfig.ENABLE_FROM_NEXT_DEFAULT, "true")
      .build();

  private final ImmutableMap<String, String> CONFIG_MAP_EMPTY = ImmutableMap
      .<String, String>builder()
      .build();

  @Test
  public void testSerde()
  {
    final SelectQueryConfig config = MAPPER.convertValue(CONFIG_MAP, SelectQueryConfig.class);
    Assert.assertEquals(false, config.getEnableFromNextDefault());

    final SelectQueryConfig config2 = MAPPER.convertValue(CONFIG_MAP2, SelectQueryConfig.class);
    Assert.assertEquals(true, config2.getEnableFromNextDefault());

    final SelectQueryConfig configEmpty = MAPPER.convertValue(CONFIG_MAP_EMPTY, SelectQueryConfig.class);
    Assert.assertEquals(true, configEmpty.getEnableFromNextDefault());
  }
}
