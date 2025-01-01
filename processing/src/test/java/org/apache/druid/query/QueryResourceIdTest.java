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

package org.apache.druid.query;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class QueryResourceIdTest
{

  @Test
  public void testConstructorWithNullString()
  {
    Assert.assertThrows(NullPointerException.class, () -> new QueryResourceId(null));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(QueryResourceId.class)
                  .withNonnullFields("queryResourceId")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testAddressableOnAssociativeMap()
  {
    Map<QueryResourceId, Integer> map = new HashMap<>();
    map.put(new QueryResourceId("abc"), 1);
    Assert.assertEquals(1, (int) map.get(new QueryResourceId("abc")));

  }
}
