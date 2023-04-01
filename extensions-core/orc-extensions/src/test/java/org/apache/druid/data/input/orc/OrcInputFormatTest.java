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

package org.apache.druid.data.input.orc;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class OrcInputFormatTest
{
  private ObjectMapper mapper;


  @Before
  public void setUp()
  {
    mapper =
        TestHelper.makeJsonMapper()
                  .registerModules(new OrcExtensionsModule().getJacksonModules())
                  .setInjectableValues(new InjectableValues.Std().addValue(Configuration.class, null));
  }

  @Test
  public void testSerdeDefault() throws Exception
  {
    final OrcInputFormat config = new OrcInputFormat(null, null, null);

    Assert.assertEquals(
        config,
        mapper.readValue(mapper.writeValueAsString(config), InputFormat.class)
    );
  }

  @Test
  public void testSerdeNonDefault() throws Exception
  {
    final OrcInputFormat config = new OrcInputFormat(new JSONPathSpec(true, Collections.emptyList()), true, null);

    Assert.assertEquals(
        config,
        mapper.readValue(mapper.writeValueAsString(config), InputFormat.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(OrcInputFormat.class)
                  .withPrefabValues(Configuration.class, new Configuration(), new Configuration())
                  .usingGetClass()
                  .verify();
  }
}
