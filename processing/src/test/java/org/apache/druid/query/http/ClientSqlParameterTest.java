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

package org.apache.druid.query.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClientSqlParameterTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    ClientSqlParameter sqlParameter = new ClientSqlParameter(
        "BIGINT",
        1234
    );
    // serde here suffers normal jackson problems e.g. if 1234 was 1234L this test would fail
    Assertions.assertEquals(
        sqlParameter,
        mapper.readValue(mapper.writeValueAsString(sqlParameter), ClientSqlParameter.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(ClientSqlParameter.class).usingGetClass().verify();
  }
}
