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

import java.sql.SQLFeatureNotSupportedException;

/**
 * Subclass of {@link SQLFeatureNotSupportedException} that is designed to simplify instantiation, using format
 * strings and values of {@link DruidSQLState}. This is the counterpart to {@link DruidJdbcException} and should be
 * thrown wherever the Druid JDBC driver does not support an optional JDBC feature, so that callers (connection pools,
 * ORMs, and other tooling) can distinguish "feature absent" from a genuine error via the standard exception type.
 */
public class DruidJdbcFeatureNotSupportedException extends SQLFeatureNotSupportedException
{
  public DruidJdbcFeatureNotSupportedException(final String formatText, final Object... arguments)
  {
    super(StringUtils.format(formatText, arguments), DruidSQLState.FeatureUnsupported.getSqlState());
  }
}
