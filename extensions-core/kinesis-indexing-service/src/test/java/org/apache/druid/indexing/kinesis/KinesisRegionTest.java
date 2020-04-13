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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class KinesisRegionTest
{
  private ObjectMapper mapper;

  @Before
  public void setupTest()
  {
    mapper = new DefaultObjectMapper();
  }

  @Test
  public void testSerde() throws IOException
  {
    KinesisRegion kinesisRegionUs1 = KinesisRegion.US_EAST_1;
    KinesisRegion kinesisRegionAp1 = KinesisRegion.AP_NORTHEAST_1;

    Assert.assertEquals("\"us-east-1\"", mapper.writeValueAsString(kinesisRegionUs1));
    Assert.assertEquals("\"ap-northeast-1\"", mapper.writeValueAsString(kinesisRegionAp1));

    KinesisRegion kinesisRegion = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                "\"us-east-1\"",
                KinesisRegion.class
            )
        ),
        KinesisRegion.class
    );

    Assert.assertEquals(kinesisRegion, KinesisRegion.US_EAST_1);
  }

  @Test
  public void testGetEndpoint()
  {
    Assert.assertEquals("kinesis.cn-north-1.amazonaws.com.cn", KinesisRegion.CN_NORTH_1.getEndpoint());
    Assert.assertEquals("kinesis.us-east-1.amazonaws.com", KinesisRegion.US_EAST_1.getEndpoint());
  }

  @Test(expected = JsonMappingException.class)
  public void testBadSerde() throws IOException
  {
    mapper.readValue(
        "\"us-east-10\"",
        KinesisRegion.class
    );
  }

}

