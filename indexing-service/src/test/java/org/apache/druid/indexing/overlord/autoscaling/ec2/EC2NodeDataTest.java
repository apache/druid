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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class EC2NodeDataTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"amiId\" : \"abc123\", \"instanceType\" : \"k2.9xsmall\", \"minInstances\" : 1, \"maxInstances\" : 2,"
                        + " \"securityGroupIds\" : [\"sg-abc321\"], \"keyName\" : \"opensesame\", \"subnetId\" : \"darknet2\","
                        + " \"associatePublicIpAddress\" : true, \"iamProfile\" : { \"name\" : \"john\", \"arn\" : \"xxx:abc:1234/xyz\" } }";
    EC2NodeData nodeData = objectMapper.readValue(json, EC2NodeData.class);

    Assert.assertEquals("abc123", nodeData.getAmiId());
    Assert.assertEquals("k2.9xsmall", nodeData.getInstanceType());
    Assert.assertEquals(2, nodeData.getMaxInstances());
    Assert.assertEquals(1, nodeData.getMinInstances());
    Assert.assertEquals(Collections.singletonList("sg-abc321"), nodeData.getSecurityGroupIds());
    Assert.assertEquals("opensesame", nodeData.getKeyName());
    Assert.assertEquals("darknet2", nodeData.getSubnetId());
    Assert.assertEquals("john", nodeData.getIamProfile().getName());
    Assert.assertEquals("xxx:abc:1234/xyz", nodeData.getIamProfile().getArn());
    Assert.assertEquals(true, nodeData.getAssociatePublicIpAddress());

    EC2NodeData nodeData2 = objectMapper.readValue("{}", EC2NodeData.class);
    // default is not always false, null has to be a valid value
    Assert.assertNull(nodeData2.getAssociatePublicIpAddress());

    // round trip
    Assert.assertEquals(
        nodeData,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(nodeData),
            EC2NodeData.class
        )
    );
  }
}
