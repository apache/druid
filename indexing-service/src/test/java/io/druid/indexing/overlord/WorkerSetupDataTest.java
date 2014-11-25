/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
