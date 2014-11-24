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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.overlord.autoscaling.ec2.EC2AutoScaler;
import io.druid.indexing.overlord.autoscaling.ec2.EC2EnvironmentConfig;
import io.druid.indexing.overlord.autoscaling.ec2.EC2NodeData;
import io.druid.indexing.overlord.autoscaling.ec2.StringEC2UserData;
import io.druid.jackson.DefaultObjectMapper;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;

public class WorkerBehaviorConfigTest
{
  @Test
  public void testSerde() throws Exception
  {
    WorkerBehaviorConfig config = new WorkerBehaviorConfig(
        new FillCapacityWithAffinityWorkerSelectStrategy(
            new FillCapacityWithAffinityConfig(
                ImmutableMap.of("foo", Arrays.asList("localhost"))
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
                    Arrays.asList("securityGroupIds"),
                    "keyNames"
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
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            return null;
          }
        }
    );
    Assert.assertEquals(config, mapper.readValue(mapper.writeValueAsBytes(config), WorkerBehaviorConfig.class));
  }
}