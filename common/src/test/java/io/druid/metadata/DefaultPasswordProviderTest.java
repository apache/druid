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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class DefaultPasswordProviderTest
{
  private static final String pwd = "nothing";
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  @Test
  public void testExplicitConstruction()
  {
    DefaultPasswordProvider pp = new DefaultPasswordProvider(pwd);
    Assert.assertEquals(pwd, pp.getPassword());
  }
  
  @Test
  public void testFromStringConstruction()
  {
    DefaultPasswordProvider pp = DefaultPasswordProvider.fromString(pwd);
    Assert.assertEquals(pwd, pp.getPassword());
  }

  @Test
  public void testDeserializationFromJsonString() throws Exception
  {
    PasswordProvider pp = jsonMapper.readValue("\"" + pwd + "\"",
        PasswordProvider.class);
    Assert.assertEquals(pwd, pp.getPassword());
  }

  @Test
  public void testDeserializationFromJson() throws Exception
  {
    PasswordProvider pp = jsonMapper.readValue(
        "{\"type\": \"default\", \"password\": \"" + pwd + "\"}",
        PasswordProvider.class);
    Assert.assertEquals(pwd, pp.getPassword());
  }
}
