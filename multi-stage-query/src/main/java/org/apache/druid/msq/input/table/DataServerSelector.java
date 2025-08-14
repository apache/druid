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

package org.apache.druid.msq.input.table;

import org.apache.druid.server.coordination.DruidServerMetadata;
import org.jboss.netty.util.internal.ThreadLocalRandom;

import java.util.Set;
import java.util.function.Function;

public enum DataServerSelector
{
  RANDOM(servers -> servers.stream()
                           .skip(ThreadLocalRandom.current().nextInt(servers.size()))
                           .findFirst()
                           .orElse(null));

  private final Function<Set<DruidServerMetadata>, DruidServerMetadata> selectServer;

  DataServerSelector(Function<Set<DruidServerMetadata>, DruidServerMetadata> selectServer)
  {
    this.selectServer = selectServer;
  }

  public Function<Set<DruidServerMetadata>, DruidServerMetadata> getSelectServerFunction()
  {
    return selectServer;
  }
}
