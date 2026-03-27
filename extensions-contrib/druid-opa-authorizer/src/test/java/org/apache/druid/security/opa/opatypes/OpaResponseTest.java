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

package org.apache.druid.security.opa.opatypes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class OpaResponseTest
{
  private final ObjectMapper mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Test
  public void testDeserializeAllowed() throws Exception
  {
    String json = "{\"result\": true}";
    OpaResponse response = mapper.readValue(json, OpaResponse.class);
    Assert.assertTrue(response.isResult());
  }

  @Test
  public void testDeserializeDenied() throws Exception
  {
    String json = "{\"result\": false}";
    OpaResponse response = mapper.readValue(json, OpaResponse.class);
    Assert.assertFalse(response.isResult());
  }

  @Test
  public void testDeserializeWithUnknownFields() throws Exception
  {
    String json = "{\"result\": true, \"decision_id\": \"12345\", \"unknown\": { \"foo\": \"bar\" }}";
    OpaResponse response = mapper.readValue(json, OpaResponse.class);
    Assert.assertTrue(response.isResult());
  }
}
