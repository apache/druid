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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SegmentWithStateTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final byte[] bytes = objectMapper.writeValueAsBytes(SegmentWithState.SegmentState.APPEND_FINISHED);
    Assert.assertEquals(
        SegmentWithState.SegmentState.APPEND_FINISHED,
        objectMapper.readValue(bytes, SegmentWithState.SegmentState.class)
    );
  }

  @Test
  public void testSerdeForBackwardCompatibility() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        SegmentWithState.SegmentState.APPENDING,
        objectMapper.readValue("\"ACTIVE\"", SegmentWithState.SegmentState.class)
    );
    Assert.assertEquals(
        SegmentWithState.SegmentState.APPEND_FINISHED,
        objectMapper.readValue("\"INACTIVE\"", SegmentWithState.SegmentState.class)
    );
  }
}
