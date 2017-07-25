/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.metadata.storage.sqlserver;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.Binding;
import org.skife.jdbi.v2.ColonPrefixNamedParamStatementRewriter;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.RewrittenStatement;
import org.skife.jdbi.v2.tweak.StatementRewriter;
import org.skife.jdbi.v2.util.StringMapper;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

@SuppressWarnings("nls")
public class SQLServerConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(SQLServerConnector.class);

  /**
   * <p>
   * <blockquote>
   *
   * <pre>
   *
   * Sql Server equivalent to the SERIAL_TYPE value in other SQLMetadataConnectors.
   *
   * SqlServer - PAYLOAD_TYPE = "VARBINARY(MAX)"
   *     Variable-length binary data
   *
   * PostgreSQL - PAYLOAD_TYPE = "BYTEA"
   *     variable-length binary string
   *
   * MySQL - PAYLOAD_TYPE = "LONGBLOB"
   *     a binary large object that can hold a variable amount of data
   *
   * </pre>
   *
   * </blockquote>
   * <p>
   *
   * @see <a href="https://msdn.microsoft.com/en-CA/library/ms187745.aspx">MS
   *      SQL Server Numeric Types</a>
   * @see io.druid.metadata.storage.postgresql.PostgreSQLConnector
   * @see io.druid.metadata.storage.mysql.MySQLConnector
   *
   */
  private static final String PAYLOAD_TYPE = "VARBINARY(MAX)";

  /**
   *
   * <p>
   * <blockquote>
   *
   * <pre>
   * Sql Server equivalent to the SERIAL_TYPE value in other SQLMetadataConnectors.
   *
   * SqlServer - SERIAL_TYPE = "[bigint] IDENTITY (1, 1)"
   *     The bigint range is from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
   *
   * PostgreSQL - SERIAL_TYPE ="BIGSERIAL"
   *     The BIGSERIAL range is from 1 to 9223372036854775807
   *
   * MySQL - SERIAL_TYPE = "BIGINT(20) AUTO_INCREMENT"
   *     The BIGINT range is from -9223372036854775808 to 9223372036854775807
   *     Also note that "SERIAL" is an alias for "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE"
   *
   * </pre>
   *
   * </blockquote>
   * <p>
   *
   * @see <a href="https://msdn.microsoft.com/en-CA/library/ms187745.aspx">MS SQL Server Numeric Types</a>
   * @see io.druid.metadata.storage.postgresql.PostgreSQLConnector
   * @see io.druid.metadata.storage.mysql.MySQLConnector
   *
   *
   *
   */
  private static final String SERIAL_TYPE = "[bigint] IDENTITY (1, 1)";

  private static final String QUOTE_STRING = "\\\"";
  public static final int DEFAULT_STREAMING_RESULT_SIZE = 100;

  private final DBI dbi;

  /**
   * <p>
   * <blockquote>
   *
   * <pre>
   * Classify Transient Sql State Codes
   * </pre>
   *
   * </blockquote>
   * <p>
   *
   * @see <a href="https://github.com/spring-projects/spring-framework/blob/v4.3.2.RELEASE/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLStateSQLExceptionTranslator.java">Spring Framework SQLStateSQLExceptionTranslator</a>
   * @see SQLException#getSQLState()
   */
  private final Set<String> TRANSIENT_SQL_CLASS_CODES = new HashSet<>(Arrays.asList(
      "08", "53", "54", "57", "58", // Resource Failures
      "JW", "JZ", "S1", // Transient Failures
      "40" // Transaction Rollback
  ));

  @Inject
  public SQLServerConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

    this.dbi = new DBI(datasource);

    this.dbi.setStatementRewriter(new CustomStatementRewriter());

    log.info("Configured Sql Server as metadata storage");
  }

  public static class CustomStatementRewriter implements StatementRewriter
  {

    private static final Pattern REWRITE_PATTERN1 = Pattern.compile("(?i)BOOLEAN NOT NULL DEFAULT FALSE");
    private static final Pattern REWRITE_PATTERN2 = Pattern.compile("(?i)BOOLEAN NOT NULL DEFAULT TRUE");
    private static final Pattern REWRITE_PATTERN3 = Pattern.compile("(?i)BOOLEAN DEFAULT FALSE");
    private static final Pattern REWRITE_PATTERN4 = Pattern.compile("(?i)BOOLEAN DEFAULT TRUE");
    private static final Pattern REWRITE_PATTERN5 = Pattern.compile("(?i)BOOLEAN");
    private static final Pattern REWRITE_PATTERN6 = Pattern.compile("(?i)TRUE");
    private static final Pattern REWRITE_PATTERN7 = Pattern.compile("(?i)FALSE");

    private ColonPrefixNamedParamStatementRewriter colonPrefixNamedParamStatementRewriter = new ColonPrefixNamedParamStatementRewriter();

    @Override
    public RewrittenStatement rewrite(String sql, Binding params, StatementContext ctx)
    {

      String currentSql = sql;
      currentSql = REWRITE_PATTERN1.matcher(currentSql).replaceAll("BIT NOT NULL DEFAULT (0)");
      currentSql = REWRITE_PATTERN2.matcher(currentSql).replaceAll("BIT NOT NULL DEFAULT (1)");
      currentSql = REWRITE_PATTERN3.matcher(currentSql).replaceAll("BIT NOT NULL DEFAULT (0)");
      currentSql = REWRITE_PATTERN4.matcher(currentSql).replaceAll("BIT NOT NULL DEFAULT (1)");
      currentSql = REWRITE_PATTERN5.matcher(currentSql).replaceAll("BIT");
      currentSql = REWRITE_PATTERN6.matcher(currentSql).replaceAll("1");
      currentSql = REWRITE_PATTERN7.matcher(currentSql).replaceAll("0");

      return (colonPrefixNamedParamStatementRewriter).rewrite(currentSql, params, ctx);

    }
  }

  @Override
  protected String getPayloadType()
  {
    return PAYLOAD_TYPE;
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public String getQuoteString()
  {
    return QUOTE_STRING;
  }

  @Override
  protected int getStreamingFetchSize()
  {
    return DEFAULT_STREAMING_RESULT_SIZE;
  }

  @Override
  public boolean tableExists(final Handle handle, final String tableName)
  {
    return !handle.createQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tableName")
        .bind("tableName", tableName)
        .map(StringMapper.FIRST)
        .list()
        .isEmpty();
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see <a href="http://stackoverflow.com/questions/1197733/does-sql-server-offer-anything-like-mysqls-on-duplicate-key-update">http://stackoverflow.com/questions/1197733/does-sql-server-offer-anything-like-mysqls-on-duplicate-key-update</a>
   *
   */
  @Override
  public Void insertOrUpdate(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key,
      final byte[] value) throws Exception
  {
    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(StringUtils.format(
                "MERGE INTO %1$s WITH (UPDLOCK, HOLDLOCK) as target"
                    + " USING "
                    + " (:key, :value) as source (%2$s, %3$s)"
                    + " ON"
                    + " (target.%2$s = source.%2$s)"
                    + " WHEN MATCHED THEN UPDATE SET %3$s = :value"
                    + " WHEN NOT MATCHED THEN INSERT (%2$s, %3$s) VALUES (:key, :value)",
                tableName,
                keyColumn,
                valueColumn))
                .bind("key", key)
                .bind("value", value)
                .execute();

            return null;
          }
        });
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see SQLException#getSQLState()
   *
   */
  @Override
  protected boolean connectorIsTransientException(Throwable e)
  {
    if (e instanceof SQLException) {
      final String sqlState = ((SQLException) e).getSQLState();
      if (sqlState == null) {
        return false;
      }

      final String sqlClassCode = sqlState.substring(0, 2);
      if (TRANSIENT_SQL_CLASS_CODES.contains(sqlClassCode)) {
        return true;
      }
    }
    return false;
  }
}
