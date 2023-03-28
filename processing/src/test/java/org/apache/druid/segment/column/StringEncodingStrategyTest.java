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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.junit.Assert;
import org.junit.Test;

public class StringEncodingStrategyTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();


  @Test
  public void testUtf8Serde() throws JsonProcessingException
  {
    StringEncodingStrategy utf8 = new StringEncodingStrategy.Utf8();
    String there = JSON_MAPPER.writeValueAsString(utf8);
    StringEncodingStrategy andBackAgain = JSON_MAPPER.readValue(there, StringEncodingStrategy.class);
    Assert.assertEquals(utf8, andBackAgain);
  }

  @Test
  public void testFrontCodedDefaultSerde() throws JsonProcessingException
  {
    StringEncodingStrategy frontCoded = new StringEncodingStrategy.FrontCoded(null, null);
    String there = JSON_MAPPER.writeValueAsString(frontCoded);
    StringEncodingStrategy andBackAgain = JSON_MAPPER.readValue(there, StringEncodingStrategy.class);
    Assert.assertEquals(frontCoded, andBackAgain);
  }

  @Test
  public void testFrontCodedFormatSerde() throws JsonProcessingException
  {
    StringEncodingStrategy frontCoded = new StringEncodingStrategy.FrontCoded(16, FrontCodedIndexed.V0);
    String there = JSON_MAPPER.writeValueAsString(frontCoded);
    StringEncodingStrategy andBackAgain = JSON_MAPPER.readValue(there, StringEncodingStrategy.class);
    Assert.assertEquals(frontCoded, andBackAgain);
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(StringEncodingStrategy.Utf8.class).usingGetClass().verify();
    EqualsVerifier.forClass(StringEncodingStrategy.FrontCoded.class).usingGetClass().verify();
  }
}
