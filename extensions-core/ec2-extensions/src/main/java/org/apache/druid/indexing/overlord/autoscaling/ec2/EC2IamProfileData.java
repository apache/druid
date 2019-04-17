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

import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EC2IamProfileData
{
  private final String name;
  private final String arn;

  @JsonCreator
  public EC2IamProfileData(
      @JsonProperty("name") String name,
      @JsonProperty("arn") String arn
  )
  {
    this.name = name;
    this.arn = arn;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getArn()
  {
    return arn;
  }

  public IamInstanceProfileSpecification toIamInstanceProfileSpecification()
  {
    final IamInstanceProfileSpecification spec = new IamInstanceProfileSpecification();
    spec.setName(name);
    spec.setArn(arn);
    return spec;
  }

  @Override
  public String toString()
  {
    return "EC2IamProfileData{" +
           "name='" + name + '\'' +
           ", arn='" + arn + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EC2IamProfileData that = (EC2IamProfileData) o;

    if (arn != null ? !arn.equals(that.arn) : that.arn != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (arn != null ? arn.hashCode() : 0);
    return result;
  }
}
