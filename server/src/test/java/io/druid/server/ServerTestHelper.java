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

package io.druid.server;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.guice.ServerModule;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;

import java.util.List;

public class ServerTestHelper
{
  public static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    final List<? extends Module> list = new ServerModule().getJacksonModules();
    for (Module module : list) {
      MAPPER.registerModule(module);
    }
    MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ObjectMapper.class.getName(), MAPPER)
            .addValue(DataSegment.PruneLoadSpecHolder.class, DataSegment.PruneLoadSpecHolder.DEFAULT)
    );
  }
}
