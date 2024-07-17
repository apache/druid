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

package org.apache.druid.msq.exec;

import com.google.inject.Inject;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.avatica.AbstractDruidJdbcStatement;
import org.apache.druid.sql.avatica.AvaticaServerConfig;
import org.apache.druid.sql.avatica.DruidMeta;
import org.apache.druid.sql.avatica.ErrorHandler;

public class MSQDruidMeta extends DruidMeta
{
  @Inject
  public MSQDruidMeta(
      final @MultiStageQuery SqlStatementFactory sqlStatementFactory,
      final AvaticaServerConfig config,
      final ErrorHandler errorHandler,
      final AuthenticatorMapper authMapper)
  {
    super(sqlStatementFactory, config, errorHandler, authMapper);
  }

  @Override
  protected ExecuteResult doFetch(AbstractDruidJdbcStatement druidStatement, int maxRows)
  {
    return super.doFetch(druidStatement, maxRows);
  }

}
