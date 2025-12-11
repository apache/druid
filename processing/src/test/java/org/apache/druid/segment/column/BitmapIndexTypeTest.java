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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class BitmapIndexTypeTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testNullsOnlySerde() throws JsonProcessingException
  {
    BitmapIndexType strategy = BitmapIndexType.NullValueIndex.INSTANCE;
    String there = JSON_MAPPER.writeValueAsString(strategy);
    BitmapIndexType andBackAgain = JSON_MAPPER.readValue(there, BitmapIndexType.class);
    Assert.assertEquals(strategy, andBackAgain);
  }

  @Test
  public void testDictionaryIdSerde() throws JsonProcessingException
  {
    BitmapIndexType strategy = BitmapIndexType.DictionaryEncodedValueIndex.INSTANCE;
    String there = JSON_MAPPER.writeValueAsString(strategy);
    BitmapIndexType andBackAgain = JSON_MAPPER.readValue(there, BitmapIndexType.class);
    Assert.assertEquals(strategy, andBackAgain);
  }
}
