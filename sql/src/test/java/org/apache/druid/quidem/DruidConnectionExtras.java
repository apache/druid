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

package org.apache.druid.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.sql.hook.DruidHookDispatcher;

import java.sql.Connection;

public interface DruidConnectionExtras
{
  ObjectMapper getObjectMapper();

  DruidHookDispatcher getDruidHookDispatcher();

  boolean isExplainSupported();

  class DruidConnectionExtrasImpl implements DruidConnectionExtras
  {
    private final ObjectMapper objectMapper;
    private final DruidHookDispatcher druidHookDispatcher;
    private final boolean isExplainSupported;

    public DruidConnectionExtrasImpl(ObjectMapper objectMapper, DruidHookDispatcher druidHookDispatcher, boolean isExplainSupported)
    {
      this.objectMapper = objectMapper;
      this.druidHookDispatcher = druidHookDispatcher;
      this.isExplainSupported = isExplainSupported;
    }

    @Override
    public ObjectMapper getObjectMapper()
    {
      return objectMapper;
    }

    @Override
    public DruidHookDispatcher getDruidHookDispatcher()
    {
      return druidHookDispatcher;
    }

    @Override
    public boolean isExplainSupported()
    {
      return isExplainSupported;
    }
  }

  static DruidConnectionExtras unwrapOrThrow(Connection connection)
  {
    if (connection instanceof DruidConnectionExtras) {
      return (DruidConnectionExtras) connection;
    }
    throw new UnsupportedOperationException("Expected DruidConnectionExtras to be implemented by connection!");
  }

}
