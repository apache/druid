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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Represents any user data that may be needed to launch EC2 instances.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "impl", defaultImpl = GalaxyEC2UserData.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "galaxy", value = GalaxyEC2UserData.class),
    @JsonSubTypes.Type(name = "string", value = StringEC2UserData.class)
})
public interface EC2UserData<T extends EC2UserData>
{
  /**
   * Return a copy of this instance with a different worker version. If no changes are needed (possibly because the
   * user data does not depend on the worker version) then it is OK to return "this".
   *
   * @param version worker version
   * @return instance with the specified version
   */
  EC2UserData<T> withVersion(String version);

  String getUserDataBase64();
}
