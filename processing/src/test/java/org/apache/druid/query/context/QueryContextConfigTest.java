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

package org.apache.druid.query.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryContextConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  void testSerde() throws Exception
  {
    assertFalse(JSON_MAPPER.readValue("{}", QueryContextConfig.class).isRejectUnknownParameters());
    assertTrue(
        JSON_MAPPER.readValue("{\"rejectUnknownParameters\": true}", QueryContextConfig.class)
                   .isRejectUnknownParameters()
    );
    assertFalse(
        JSON_MAPPER.readValue("{\"rejectUnknownParameters\": false}", QueryContextConfig.class)
                   .isRejectUnknownParameters()
    );
  }

  @Test
  void testDefault()
  {
    assertFalse(QueryContextConfig.DEFAULT.isRejectUnknownParameters());
  }
}
