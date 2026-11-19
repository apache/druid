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

package org.apache.druid.jdbc;

import java.sql.SQLException;

/**
 * Subclass of {@link SQLException} that is designed to simplify instantiation, using format strings and values
 * of {@link DruidSQLState}. It is not guaranteed that *all* exceptions thrown by the Druid JDBC driver are
 * instances of this class.
 */
public class DruidJdbcException extends SQLException
{
  public DruidJdbcException(final String formatText, final Object... arguments)
  {
    super(StringUtils.format(formatText, arguments));
  }

  public DruidJdbcException(final Throwable cause, final String formatText, final Object... arguments)
  {
    super(StringUtils.format(formatText, arguments), cause);
  }

  public DruidJdbcException(final DruidSQLState sqlState, final String formatText, final Object... arguments)
  {
    super(StringUtils.format(formatText, arguments), sqlState.getSqlState());
  }

  public DruidJdbcException(
      final Throwable cause,
      final DruidSQLState sqlState,
      final String formatText,
      final Object... arguments
  )
  {
    super(StringUtils.format(formatText, arguments), sqlState.getSqlState(), cause);
  }
}
