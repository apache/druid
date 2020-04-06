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

package org.apache.druid.security.pac4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class Pac4jCommonConfigTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();

    String jsonStr = "{\n"
                     + "  \"cookiePassphrase\": \"testpass\",\n"
                     + "  \"readTimeout\": \"PT10S\",\n"
                     + "  \"enableCustomSslContext\": true\n"
                     + "}\n";

    Pac4jCommonConfig conf = jsonMapper.readValue(
        jsonMapper.writeValueAsString(jsonMapper.readValue(jsonStr, Pac4jCommonConfig.class)),
        Pac4jCommonConfig.class
    );

    Assert.assertEquals("testpass", conf.getCookiePassphrase().getPassword());
    Assert.assertEquals(10_000L, conf.getReadTimeout().getMillis());
    Assert.assertTrue(conf.isEnableCustomSslContext());
  }
}
