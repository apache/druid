/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import com.google.common.base.Charsets;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.overlord.autoscaling.ec2.EC2UserData;
import io.druid.indexing.overlord.autoscaling.ec2.GalaxyEC2UserData;
import io.druid.indexing.overlord.autoscaling.ec2.StringEC2UserData;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

@Deprecated
public class WorkerSetupDataTest
{
  @Test
  public void testGalaxyEC2UserDataSerde() throws IOException
  {
    final String json = "{\"env\":\"druid\",\"version\":null,\"type\":\"typical\"}";
    final GalaxyEC2UserData userData = (GalaxyEC2UserData) TestUtils.MAPPER.readValue(json, EC2UserData.class);
    Assert.assertEquals("druid", userData.getEnv());
    Assert.assertEquals("typical", userData.getType());
    Assert.assertNull(userData.getVersion());
    Assert.assertEquals("1234", userData.withVersion("1234").getVersion());
  }

  @Test
  public void testStringEC2UserDataSerde() throws IOException
  {
    final String json = "{\"impl\":\"string\",\"data\":\"hey :ver:\",\"versionReplacementString\":\":ver:\",\"version\":\"1234\"}";
    final StringEC2UserData userData = (StringEC2UserData) TestUtils.MAPPER.readValue(json, EC2UserData.class);
    Assert.assertEquals("hey :ver:", userData.getData());
    Assert.assertEquals("1234", userData.getVersion());
    Assert.assertEquals(
        Base64.encodeBase64String("hey 1234".getBytes(Charsets.UTF_8)),
        userData.getUserDataBase64()
    );
    Assert.assertEquals(
        Base64.encodeBase64String("hey xyz".getBytes(Charsets.UTF_8)),
        userData.withVersion("xyz").getUserDataBase64()
    );
  }
}
