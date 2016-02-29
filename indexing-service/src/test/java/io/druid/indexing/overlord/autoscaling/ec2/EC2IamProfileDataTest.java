/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.autoscaling.ec2;

import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

public class EC2IamProfileDataTest
{
  @Test
  public void testUsesOnlyArnIfBothSet() throws Exception
  {
    final EC2IamProfileData iamProfileData = new EC2IamProfileData("name", "arn");
    final IamInstanceProfileSpecification spec = iamProfileData.toIamInstanceProfileSpecification();
    assert(Objects.equals(spec.getArn(), "arn"));
    assert(spec.getName() == null);
  }

  @Test
  public void testUsesNameIfArnNotSet() throws Exception
  {
    final EC2IamProfileData iamProfileData = new EC2IamProfileData("name", null);
    final IamInstanceProfileSpecification spec = iamProfileData.toIamInstanceProfileSpecification();
    assert(Objects.equals(spec.getName(), "name"));
    assert(spec.getArn() == null);
  }
}

