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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.query.QueryContext;
import org.junit.Assert;
import org.junit.Test;

public class MSQModeTest
{

  @Test
  public void testPopulateQueryContextWhenNoSupercedingValuePresent()
  {
    QueryContext originalQueryContext = new QueryContext();
    MSQMode.populateDefaultQueryContext("strict", originalQueryContext);
    Assert.assertEquals(ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0), originalQueryContext.getMergedParams());
  }

  @Test
  public void testPopulateQueryContextWhenSupercedingValuePresent()
  {
    QueryContext originalQueryContext = new QueryContext(ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10));
    MSQMode.populateDefaultQueryContext("strict", originalQueryContext);
    Assert.assertEquals(ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10), originalQueryContext.getMergedParams());

  }

  @Test
  public void testPopulateQueryContextWhenInvalidMode()
  {
    QueryContext originalQueryContext = new QueryContext();
    Assert.assertThrows(ISE.class, () -> {
      MSQMode.populateDefaultQueryContext("fake_mode", originalQueryContext);
    });
  }
}
