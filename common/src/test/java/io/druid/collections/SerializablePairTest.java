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

package io.druid.collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SerializablePairTest
{
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  @Test
  public void testBytesSerde() throws IOException
  {
    SerializablePair pair = new SerializablePair<>(5L, 9L);
    byte[] bytes = jsonMapper.writeValueAsBytes(pair);
    SerializablePair<Number, Number> deserializedPair = jsonMapper.readValue(bytes, SerializablePair.class);
    Assert.assertEquals(pair.lhs, deserializedPair.lhs.longValue());
    Assert.assertEquals(pair.rhs, deserializedPair.rhs.longValue());
  }

  @Test
  public void testStringSerde() throws IOException
  {
    SerializablePair pair = new SerializablePair<>(5L, 9L);
    String str = jsonMapper.writeValueAsString(pair);
    SerializablePair<Number, Number> deserializedPair = jsonMapper.readValue(str, SerializablePair.class);
    Assert.assertEquals(pair.lhs, deserializedPair.lhs.longValue());
    Assert.assertEquals(pair.rhs, deserializedPair.rhs.longValue());
  }
}
