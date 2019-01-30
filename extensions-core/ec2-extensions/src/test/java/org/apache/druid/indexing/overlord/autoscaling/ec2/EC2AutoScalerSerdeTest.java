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

package org.apache.druid.indexing.overlord.autoscaling.ec2;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

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
                      + "         \"subnetId\" : \"redkeep\",\n"
                      + "         \"iamProfile\" : {\"name\": \"foo\", \"arn\": \"bar\"}\n"
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

  private static void verifyAutoScaler(final EC2AutoScaler autoScaler)
  {
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
        Collections.singletonList("kingsguard"),
        autoScaler.getEnvConfig().getNodeData().getSecurityGroupIds()
    );
    Assert.assertEquals("redkeep", autoScaler.getEnvConfig().getNodeData().getSubnetId());
    Assert.assertEquals(
        "foo",
        autoScaler.getEnvConfig()
                  .getNodeData()
                  .getIamProfile()
                  .toIamInstanceProfileSpecification()
                  .getName()
    );
    Assert.assertEquals(
        "bar",
        autoScaler.getEnvConfig()
                  .getNodeData()
                  .getIamProfile()
                  .toIamInstanceProfileSpecification()
                  .getArn()
    );

    // userData
    Assert.assertEquals(
        "VERSION=1234\n",
        StringUtils.fromUtf8(
            StringUtils
                .decodeBase64String(autoScaler.getEnvConfig().getUserData().withVersion("1234").getUserDataBase64())
        )
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules((Iterable<Module>) new EC2Module().getJacksonModules());
    objectMapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object o,
              DeserializationContext deserializationContext,
              BeanProperty beanProperty,
              Object o1
          )
          {
            return null;
          }
        }
    );

    final EC2AutoScaler autoScaler = (EC2AutoScaler) objectMapper.readValue(json, AutoScaler.class);
    verifyAutoScaler(autoScaler);

    final EC2AutoScaler roundTripAutoScaler = (EC2AutoScaler) objectMapper.readValue(
        objectMapper.writeValueAsBytes(autoScaler),
        AutoScaler.class
    );
    verifyAutoScaler(roundTripAutoScaler);

    Assert.assertEquals("Round trip equals", autoScaler, roundTripAutoScaler);
  }
}
