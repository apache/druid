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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ClientCompactionTaskDimensionsSpecTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ClientCompactionTaskDimensionsSpec.class)
                  .withPrefabValues(
                      DimensionSchema.class,
                      new StringDimensionSchema("bar", DimensionSchema.MultiValueHandling.ofDefault(), true),
                      new StringDimensionSchema("foo", DimensionSchema.MultiValueHandling.ofDefault(), true)
                  )
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final ClientCompactionTaskDimensionsSpec expected = new ClientCompactionTaskDimensionsSpec(
        DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))
    );
    final ObjectMapper mapper = new ObjectMapper();
    final byte[] json = mapper.writeValueAsBytes(expected);
    final ClientCompactionTaskDimensionsSpec fromJson = (ClientCompactionTaskDimensionsSpec) mapper.readValue(
        json,
        ClientCompactionTaskDimensionsSpec.class
    );
    Assert.assertEquals(expected, fromJson);
  }

  @Test(expected = ParseException.class)
  public void testInvalidDimensionsField()
  {
    final ClientCompactionTaskDimensionsSpec expected = new ClientCompactionTaskDimensionsSpec(
        DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim", "dim"))
    );
  }
}
