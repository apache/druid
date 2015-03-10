/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import io.druid.indexing.overlord.autoscaling.ec2.EC2AutoScaler;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class EC2AutoScalerSerdeTest
{
  final String json = "{\n"
                      + "   \"envConfig\" : {\n"
                      + "      \"availabilityZone\" : \"westeros-east-1a\",\n"
                      + "      \"nodeData\" : {\n"
                      + "         \"amiId\" : \"ami-abc\",\n"
                      + "         \"instanceType\" : \"t1.micro\",\n"
                      + "         \"keyName\" : \"iron\",\n"
                      + "         \"maxInstances\" : 1,\n"
                      + "         \"minInstances\" : 1,\n"
                      + "         \"securityGroupIds\" : [\"kingsguard\"],\n"
                      + "         \"subnetId\" : \"redkeep\"\n"
                      + "      },\n"
                      + "      \"userData\" : {\n"
                      + "         \"data\" : \"VERSION=:VERSION:\\n\","
                      + "         \"impl\" : \"string\",\n"
                      + "         \"versionReplacementString\" : \":VERSION:\"\n"
                      + "      }\n"
                      + "   },\n"
                      + "   \"maxNumWorkers\" : 3,\n"
                      + "   \"minNumWorkers\" : 2,\n"
                      + "   \"type\" : \"ec2\"\n"
                      + "}";

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object o, DeserializationContext deserializationContext, BeanProperty beanProperty, Object o1
          )
          {
            return null;
          }
        }
    );

    final EC2AutoScaler autoScaler = objectMapper.readValue(json, EC2AutoScaler.class);

    Assert.assertEquals(3, autoScaler.getMaxNumWorkers());
    Assert.assertEquals(2, autoScaler.getMinNumWorkers());
    Assert.assertEquals("westeros-east-1a", autoScaler.getEnvConfig().getAvailabilityZone());

    // nodeData
    Assert.assertEquals("ami-abc", autoScaler.getEnvConfig().getNodeData().getAmiId());
    Assert.assertEquals("t1.micro", autoScaler.getEnvConfig().getNodeData().getInstanceType());
    Assert.assertEquals("iron", autoScaler.getEnvConfig().getNodeData().getKeyName());
    Assert.assertEquals(1, autoScaler.getEnvConfig().getNodeData().getMaxInstances());
    Assert.assertEquals(1, autoScaler.getEnvConfig().getNodeData().getMinInstances());
    Assert.assertEquals(
        Lists.newArrayList("kingsguard"),
        autoScaler.getEnvConfig().getNodeData().getSecurityGroupIds()
    );
    Assert.assertEquals("redkeep", autoScaler.getEnvConfig().getNodeData().getSubnetId());

    // userData
    Assert.assertEquals(
        "VERSION=1234\n",
        new String(
            BaseEncoding.base64()
                        .decode(autoScaler.getEnvConfig().getUserData().withVersion("1234").getUserDataBase64()),
            Charsets.UTF_8
        )
    );

    // Round trip.
    Assert.assertEquals(
        "Round trip",
        autoScaler,
        objectMapper.readValue(objectMapper.writeValueAsBytes(autoScaler), EC2AutoScaler.class)
    );
  }
}
