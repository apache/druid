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

package org.apache.druid.data.input.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class S3InputSourceConfigTest
{
  @Test
  public void testSerdeAccessSecretKey() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final S3InputSourceConfig config = new S3InputSourceConfig(
        new DefaultPasswordProvider("the-access-key"),
        new DefaultPasswordProvider("the-secret-key"),
        null,
        null
    );

    Assert.assertEquals(
        config,
        mapper.readValue(mapper.writeValueAsString(config), S3InputSourceConfig.class)
    );
  }

  @Test
  public void testSerdeAssumeRole() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final S3InputSourceConfig config = new S3InputSourceConfig(
        null,
        null,
        "the-role-arn",
        "the-role-external-id"
    );

    Assert.assertEquals(
        config,
        mapper.readValue(mapper.writeValueAsString(config), S3InputSourceConfig.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(S3InputSourceConfig.class).usingGetClass().verify();
  }
}
