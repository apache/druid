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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.junit.Assert;
import org.junit.Test;

public class MSQTuningConfigTest
{
  @Test
  public void testSerdeDefault() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    Assert.assertEquals(
        MSQTuningConfig.defaultConfig(),
        mapper.readValue(
            mapper.writeValueAsString(MSQTuningConfig.defaultConfig()),
            MSQTuningConfig.class
        )
    );
  }

  @Test
  public void testSerdeNonDefault() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final MSQTuningConfig config = new MSQTuningConfig(
        2,
        3,
        4,
        new IndexSpec(
            null,
            null,
            new StringEncodingStrategy.FrontCoded(null),
            null,
            null,
            null,
            null
        )
    );

    Assert.assertEquals(config, mapper.readValue(mapper.writeValueAsString(config), MSQTuningConfig.class));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(MSQTuningConfig.class).usingGetClass().verify();
  }
}
