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

package org.apache.druid.java.util.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class StringEncodingTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();

    Assert.assertEquals(
        StringEncoding.UTF16LE,
        mapper.readValue(mapper.writeValueAsString(StringEncoding.UTF16LE), StringEncoding.class)
    );

    Assert.assertEquals(
        StringEncoding.UTF8,
        mapper.readValue(mapper.writeValueAsString(StringEncoding.UTF8), StringEncoding.class)
    );
  }

  @Test
  public void testGetCacheKey()
  {
    Assert.assertFalse(
        Arrays.equals(
            StringEncoding.UTF8.getCacheKey(),
            StringEncoding.UTF16LE.getCacheKey()
        )
    );
  }
}
