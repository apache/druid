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

package org.apache.druid.server.system.table;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;

import java.util.Collections;
import java.util.Set;

/** Descriptor for the native {@code sys.tasks} table. */
public class TaskTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "tasks";
  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ColumnType.STRING)
      .add("group_id", ColumnType.STRING)
      .add("type", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("created_time", ColumnType.STRING)
      .add("queue_insertion_time", ColumnType.STRING)
      .add("status", ColumnType.STRING)
      .add("runner_status", ColumnType.STRING)
      .add("duration", ColumnType.LONG)
      .add("location", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("plaintext_port", ColumnType.LONG)
      .add("tls_port", ColumnType.LONG)
      .add("error_msg", ColumnType.STRING)
      .build();

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.OVERLORD);
  private static final int DATASOURCE_COLUMN = ROW_SIGNATURE.indexOf("datasource");
  private static final SystemTableRowAuthorizer ROW_AUTHORIZER = new SystemTableRowAuthorizer()
  {
    @Override
    public Iterable<Object[]> filterAuthorizedRows(
        final Iterable<Object[]> rows,
        final AuthenticationResult authenticationResult,
        final AuthorizerMapper authorizerMapper
    )
    {
      return AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          rows,
          row -> Collections.singletonList(
              AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply((String) row[DATASOURCE_COLUMN])
          ),
          authorizerMapper
      );
    }
  };

  @Override
  public String getTableName()
  {
    return TABLE_NAME;
  }

  @Override
  public Set<NodeRole> getNodeRoles()
  {
    return NODE_ROLES;
  }

  @Override
  public SystemTableRoutingMode getRoutingMode()
  {
    return SystemTableRoutingMode.LEADER_ONLY;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return ROW_SIGNATURE;
  }

  @Override
  public SystemTableRowAuthorizer getRowAuthorizer()
  {
    return ROW_AUTHORIZER;
  }
}
