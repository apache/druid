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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ResidualFilterModeTest
{
  @Test
  public void testFromString()
  {
    Assert.assertEquals(ResidualFilterMode.IGNORE, ResidualFilterMode.fromString("ignore"));
    Assert.assertEquals(ResidualFilterMode.FAIL, ResidualFilterMode.fromString("fail"));

    // Test case insensitivity
    Assert.assertEquals(ResidualFilterMode.IGNORE, ResidualFilterMode.fromString("IGNORE"));
    Assert.assertEquals(ResidualFilterMode.FAIL, ResidualFilterMode.fromString("FAIL"));
  }

  @Test
  public void testFromStringInvalid()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ResidualFilterMode.fromString("invalid")
    );
  }

  @Test
  public void testGetValue()
  {
    Assert.assertEquals("ignore", ResidualFilterMode.IGNORE.getValue());
    Assert.assertEquals("fail", ResidualFilterMode.FAIL.getValue());
  }

  @Test
  public void testJsonSerialization() throws Exception
  {
    ObjectMapper mapper = new ObjectMapper();

    // Test serialization
    Assert.assertEquals("\"ignore\"", mapper.writeValueAsString(ResidualFilterMode.IGNORE));
    Assert.assertEquals("\"fail\"", mapper.writeValueAsString(ResidualFilterMode.FAIL));

    // Test deserialization
    Assert.assertEquals(ResidualFilterMode.IGNORE, mapper.readValue("\"ignore\"", ResidualFilterMode.class));
    Assert.assertEquals(ResidualFilterMode.FAIL, mapper.readValue("\"fail\"", ResidualFilterMode.class));
  }
}
