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

package org.apache.druid.msq.indexing;

import org.apache.druid.cli.CliIndexer;
import org.apache.druid.cli.CliPeon;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import java.util.Collections;
import java.util.List;

/**
 * Production implementation of {@link ResourcePermissionMapper} for tasks: {@link CliIndexer} and {@link CliPeon}.
 */
public class IndexerResourcePermissionMapper implements ResourcePermissionMapper
{
  private final String dataSource;

  public IndexerResourcePermissionMapper(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Override
  public List<ResourceAction> getAdminPermissions()
  {
    return Collections.singletonList(
        new ResourceAction(
            new Resource(dataSource, ResourceType.DATASOURCE),
            Action.WRITE
        )
    );
  }
}
