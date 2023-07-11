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

package org.apache.druid.msq.input.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class ExternalInputSliceTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    final ExternalInputSlice slice = new ExternalInputSlice(
        Collections.singletonList(
            new LocalInputSource(
                null,
                null,
                Collections.singletonList(new File("/nonexistent/file"))
            )
        ),
        ExternalInputSpecSlicerTest.INPUT_FORMAT,
        ExternalInputSpecSlicerTest.SIGNATURE
    );

    Assert.assertEquals(
        slice,
        mapper.readValue(mapper.writeValueAsString(slice), InputSlice.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ExternalInputSlice.class).usingGetClass().verify();
  }
}
