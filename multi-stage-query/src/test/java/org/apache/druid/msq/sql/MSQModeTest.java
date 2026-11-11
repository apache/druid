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

package org.apache.druid.msq.sql;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class MSQModeTest
{
  @Test
  public void testPopulateQueryContextWhenNoSupercedingValuePresent()
  {
    Map<String, Object> originalQueryContext = new HashMap<>();
    MSQMode.populateDefaultQueryContext("strict", originalQueryContext);
    Assertions.assertEquals(QueryContext.ofMap(QueryContextParameters.MAX_PARSE_EXCEPTIONS, 0L), originalQueryContext);
  }

  @Test
  public void testPopulateQueryContextWhenSupercedingValuePresent()
  {
    Map<String, Object> originalQueryContext = new HashMap<>();
    QueryContextParameters.MAX_PARSE_EXCEPTIONS.set(originalQueryContext, 10L);
    MSQMode.populateDefaultQueryContext("strict", originalQueryContext);
    Assertions.assertEquals(QueryContext.ofMap(QueryContextParameters.MAX_PARSE_EXCEPTIONS, 10L), originalQueryContext);

  }

  @Test
  public void testPopulateQueryContextWhenInvalidMode()
  {
    Map<String, Object> originalQueryContext = new HashMap<>();
    Assertions.assertThrows(ISE.class, () ->
      MSQMode.populateDefaultQueryContext("fake_mode", originalQueryContext));
  }
}
