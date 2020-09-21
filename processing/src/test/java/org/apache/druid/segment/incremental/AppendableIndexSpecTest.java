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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class AppendableIndexSpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerializationOnheapString() throws Exception
  {
    AppendableIndexSpec spec = JSON_MAPPER.readValue(
        "{\"type\": \"onheap\"}", AppendableIndexSpec.class
    );
    Assert.assertTrue(spec instanceof OnheapIncrementalIndex.Spec);
  }

  @Test
  public void testSerializationOffheapString() throws Exception
  {
    AppendableIndexSpec spec = JSON_MAPPER.readValue(
        "{\"type\": \"offheap\"}", AppendableIndexSpec.class
    );
    Assert.assertTrue(spec instanceof OffheapIncrementalIndex.Spec);
  }

  @Test
  public void testSerializationOffheapStringWithZeroSizes() throws Exception
  {
    AppendableIndexSpec spec = JSON_MAPPER.readValue(
        "{\"type\": \"offheap\", \"bufferSize\": 0, \"cacheSize\": 0}", AppendableIndexSpec.class
    );
    Assert.assertTrue(spec instanceof OffheapIncrementalIndex.Spec);
  }

  @Test
  public void testSerializationOffheapStringWithSizes() throws Exception
  {
    AppendableIndexSpec spec = JSON_MAPPER.readValue(
        "{\"type\": \"offheap\", \"bufferSize\": 1048576, \"cacheSize\": 1073741824}",
        AppendableIndexSpec.class
    );
    Assert.assertTrue(spec instanceof OffheapIncrementalIndex.Spec);
    Assert.assertEquals(1048576, ((OffheapIncrementalIndex.Spec) spec).bufferSize);
    Assert.assertEquals(1073741824, ((OffheapIncrementalIndex.Spec) spec).cacheSize);
  }
}
