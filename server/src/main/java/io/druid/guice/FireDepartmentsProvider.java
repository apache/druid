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

package io.druid.guice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.segment.realtime.FireDepartment;

import java.util.List;

/**
 */
public class FireDepartmentsProvider implements Provider<List<FireDepartment>>
{
  private final List<FireDepartment> fireDepartments = Lists.newArrayList();

  @Inject
  public FireDepartmentsProvider(
      ObjectMapper jsonMapper,
      RealtimeManagerConfig config
  )
  {
    try {
      this.fireDepartments.addAll(
          (List<FireDepartment>) jsonMapper.readValue(
              config.getSpecFile(), new TypeReference<List<FireDepartment>>()
          {
          }
          )
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  @Override
  public List<FireDepartment> get()
  {
    return fireDepartments;
  }
}
