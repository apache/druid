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

package org.apache.druid.client.selector;

import org.apache.druid.client.DruidServer;
import org.apache.druid.query.QueryRunner;

/**
 */
public class QueryableDruidServer<T extends QueryRunner>
{
  private final DruidServer server;
  private final T queryRunner;

  public QueryableDruidServer(DruidServer server, T queryRunner)
  {
    this.server = server;
    this.queryRunner = queryRunner;
  }

  public DruidServer getServer()
  {
    return server;
  }

  public T getQueryRunner()
  {
    return queryRunner;
  }

  @Override
  public String toString()
  {
    return "QueryableDruidServer{" +
           "server=" + server +
           ", queryRunner=" + queryRunner +
           '}';
  }
}
