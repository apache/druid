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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;

/**
 * A class containing unit tests for testing implementations of {@link org.apache.calcite.sql.SqlNode#unparse(SqlWriter, int, int)}
 * in custom Druid SqlNode classes, like {@link DruidSqlInsert} and {@link DruidSqlReplace}.
 */
public class DruidSqlUnparseTest
{
  @Test
  public void testUnparseInsert() throws ParseException
  {
    String sqlQuery = "INSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME";
    String prettySqlQuery = "INSERT INTO \"dst\"\n"
                     + "SELECT *\n"
                     + "    FROM \"foo\"\n"
                     + "PARTITIONED BY ALL TIME";

    DruidSqlParserImpl druidSqlParser = createTestParser(sqlQuery);
    DruidSqlInsert druidSqlReplace = (DruidSqlInsert) druidSqlParser.DruidSqlInsertEof();

    druidSqlReplace.unparse(sqlWriter, 0, 0);
    assertEquals(prettySqlQuery, sqlWriter.toSqlString().getSql());
  }

  @Test
  public void testUnparseReplaceAll() throws ParseException
  {
    String sqlQuery = "REPLACE INTO dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME CLUSTERED BY dim1";
    String prettySqlQuery = "REPLACE INTO \"dst\"\n"
                            + "OVERWRITE ALL\n"
                            + "SELECT *\n"
                            + "    FROM \"foo\"\n"
                            + "PARTITIONED BY ALL TIME "
                            + "CLUSTERED BY \"dim1\"";

    DruidSqlParserImpl druidSqlParser = createTestParser(sqlQuery);
    DruidSqlReplace druidSqlReplace = (DruidSqlReplace) druidSqlParser.DruidSqlReplaceEof();

    druidSqlReplace.unparse(sqlWriter, 0, 0);
    assertEquals(prettySqlQuery, sqlWriter.toSqlString().getSql());
  }

  @Test
  public void testUnparseReplaceWhere() throws ParseException
  {
    String sqlQuery = "REPLACE INTO dst OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00' SELECT * FROM foo PARTITIONED BY DAY CLUSTERED BY dim1";
    String prettySqlQuery = "REPLACE INTO \"dst\"\n"
                            + "OVERWRITE \"__time\" >= TIMESTAMP '2000-01-01 00:00:00' AND \"__time\" < TIMESTAMP '2000-01-02 00:00:00'\n"
                            + "SELECT *\n"
                            + "    FROM \"foo\"\n"
                            + "PARTITIONED BY DAY "
                            + "CLUSTERED BY \"dim1\"";
    DruidSqlParserImpl druidSqlParser = createTestParser(sqlQuery);
    DruidSqlReplace druidSqlReplace = (DruidSqlReplace) druidSqlParser.DruidSqlReplaceEof();
    druidSqlReplace.unparse(sqlWriter, 0, 0);
    assertEquals(prettySqlQuery, sqlWriter.toSqlString().getSql());
  }

  private final SqlWriter sqlWriter = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);

  private static DruidSqlParserImpl createTestParser(String parseString)
  {
    DruidSqlParserImplFactory druidSqlParserImplFactory = new DruidSqlParserImplFactory();
    DruidSqlParserImpl druidSqlParser = (DruidSqlParserImpl) druidSqlParserImplFactory.getParser(new StringReader(parseString));
    druidSqlParser.setUnquotedCasing(Casing.TO_LOWER);
    druidSqlParser.setQuotedCasing(Casing.TO_LOWER);
    druidSqlParser.setIdentifierMaxLength(20);
    return druidSqlParser;
  }

  @Test
  public void testUnparseExternalSqlIdentifierReplace() throws ParseException
  {
    String sqlQuery = "REPLACE INTO EXTERN( s3(bucket=>'bucket1',prefix=>'prefix1') ) AS CSV OVERWRITE ALL SELECT dim2 FROM foo";
    String prettySqlQuery = "REPLACE INTO EXTERN(S3(bucket => 'bucket1', prefix => 'prefix1'))\n"
                            + "AS csv\n"
                            + "OVERWRITE ALL\n"
                            + "SELECT \"dim2\"\n"
                            + "    FROM \"foo\"\n";
    DruidSqlParserImpl druidSqlParser = createTestParser(sqlQuery);
    DruidSqlReplace druidSqlReplace = (DruidSqlReplace) druidSqlParser.DruidSqlReplaceEof();
    druidSqlReplace.unparse(sqlWriter, 0, 0);
    assertEquals(prettySqlQuery, sqlWriter.toSqlString().getSql());
  }

  @Test
  public void testUnparseExternalSqlIdentifierInsert() throws ParseException
  {
    String sqlQuery = "INSERT INTO EXTERN( s3(bucket=>'bucket1',prefix=>'prefix1') ) AS CSV SELECT dim2 FROM foo";
    String prettySqlQuery = "INSERT INTO EXTERN(S3(bucket => 'bucket1', prefix => 'prefix1'))\n"
                            + "AS csv\n"
                            + "SELECT \"dim2\"\n"
                            + "    FROM \"foo\"\n";
    DruidSqlParserImpl druidSqlParser = createTestParser(sqlQuery);
    DruidSqlInsert druidSqlInsert = (DruidSqlInsert) druidSqlParser.DruidSqlInsertEof();
    druidSqlInsert.unparse(sqlWriter, 0, 0);
    assertEquals(prettySqlQuery, sqlWriter.toSqlString().getSql());
  }
}
