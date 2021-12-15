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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class UserCompactionTaskTransformConfigTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(UserCompactionTaskTransformConfig.class)
                  .withNonnullFields("filter")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    NullHandling.initializeForTests();
    final UserCompactionTaskTransformConfig expected = new UserCompactionTaskTransformConfig(
        new SelectorDimFilter("dim1", "foo", null)
    );
    final ObjectMapper mapper = new DefaultObjectMapper();
    final byte[] json = mapper.writeValueAsBytes(expected);
    final UserCompactionTaskTransformConfig fromJson = (UserCompactionTaskTransformConfig) mapper.readValue(
        json,
        UserCompactionTaskTransformConfig.class
    );
    Assert.assertEquals(expected, fromJson);
  }
}
