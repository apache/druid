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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.overlord.autoscaling.ec2.EC2AutoScaler;
import org.apache.druid.indexing.overlord.autoscaling.ec2.EC2EnvironmentConfig;
import org.apache.druid.indexing.overlord.autoscaling.ec2.EC2NodeData;
import org.apache.druid.indexing.overlord.autoscaling.ec2.StringEC2UserData;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class WorkerBehaviorConfigTest
{
  @Test
  public void testSerde() throws Exception
  {
    DefaultWorkerBehaviorConfig config = new DefaultWorkerBehaviorConfig(
        new FillCapacityWithAffinityWorkerSelectStrategy(
            new AffinityConfig(
                ImmutableMap.of("foo", ImmutableSet.of("localhost")),
                false
            )
        ),
        new EC2AutoScaler(
            7,
            11,
            new EC2EnvironmentConfig(
                "us-east-1a",
                new EC2NodeData(
                    "amiid",
                    "instanceType",
                    3,
                    5,
                    Collections.singletonList("securityGroupIds"),
                    "keyNames",
                    "subnetId",
                    null,
                    null
                ),
                new StringEC2UserData(
                    "availZone",
                    "replace",
                    "version"
                )
            ),
            null,
            null
        )
    );

    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(EC2AutoScaler.class);
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId,
              DeserializationContext ctxt,
              BeanProperty forProperty,
              Object beanInstance
          )
          {
            return null;
          }
        }
    );
    Assert.assertEquals(config, mapper.readValue(mapper.writeValueAsBytes(config), DefaultWorkerBehaviorConfig.class));
  }
}
