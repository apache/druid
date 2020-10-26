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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FalseDimFilterTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final FalseDimFilter original = FalseDimFilter.instance();
    final byte[] bytes = mapper.writeValueAsBytes(original);
    final FalseDimFilter fromBytes = (FalseDimFilter) mapper.readValue(bytes, DimFilter.class);
    Assert.assertSame(original, fromBytes);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(FalseDimFilter.class)
                  .usingGetClass()
                  .withIgnoredFields("cachedOptimizedFilter")
                  .verify();
  }
}
