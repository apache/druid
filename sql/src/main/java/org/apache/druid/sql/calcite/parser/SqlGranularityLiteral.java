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

package org.apache.druid.sql.calcite.parser;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nonnull;

/**
 * Extends the {@link SqlLiteral} to hold parameters for the PARTITIONED BY clause.
 */
public class SqlGranularityLiteral extends SqlLiteral
{
  private final String unparseString;
  private final Granularity granularity;

  public SqlGranularityLiteral(
      @Nonnull Granularity granularity,
      @Nonnull String unparseString,
      SqlParserPos pos)
  {
    super(new NlsString(unparseString, null, null), SqlTypeName.CHAR, pos);
    this.granularity = granularity;
    this.unparseString = unparseString;
  }

  @Override
  public SqlGranularityLiteral clone(SqlParserPos pos)
  {
    return new SqlGranularityLiteral(granularity, unparseString, pos);
  }

  @Nonnull
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword(unparseString);
  }
}
