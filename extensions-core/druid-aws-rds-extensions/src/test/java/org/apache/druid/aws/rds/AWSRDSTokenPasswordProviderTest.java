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

package org.apache.druid.aws.rds;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.metadata.PasswordProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AWSRDSTokenPasswordProviderTest
{
  @Test
  public void testSerde() throws IOException
  {
    ObjectMapper jsonMapper = new ObjectMapper();

    for (Module module : new AWSRDSModule().getJacksonModules()) {
      jsonMapper.registerModule(module);
    }

    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(AWSCredentialsProvider.class, new AWSCredentialsProvider()
        {
          @Override
          public AWSCredentials getCredentials()
          {
            return null;
          }

          @Override
          public void refresh()
          {

          }
        })
    );

    String jsonStr = "{\n"
                     + "  \"type\": \"aws-rds-token\",\n"
                     + "  \"user\": \"testuser\",\n"
                     + "  \"host\": \"testhost\",\n"
                     + "  \"port\": 5273,\n"
                     + "  \"region\": \"testregion\"\n"
                     + "}\n";

    PasswordProvider pp = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, PasswordProvider.class)
        ),
        PasswordProvider.class
    );

    AWSRDSTokenPasswordProvider awsPwdProvider = (AWSRDSTokenPasswordProvider) pp;
    Assert.assertEquals("testuser", awsPwdProvider.getUser());
    Assert.assertEquals("testhost", awsPwdProvider.getHost());
    Assert.assertEquals(5273, awsPwdProvider.getPort());
    Assert.assertEquals("testregion", awsPwdProvider.getRegion());
  }
}
