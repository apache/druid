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

package io.druid.security.basic.db;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.metadata.BaseSQLMetadataConnector;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class SQLBasicAuthenticatorStorageConnector
    extends BaseSQLMetadataConnector
    implements BasicAuthenticatorStorageConnector
{
  public static final String USERS = "users";
  public static final String USER_CREDENTIALS = "user_credentials";
  public static final List<String> TABLE_NAMES = Lists.newArrayList(
      USER_CREDENTIALS,
      USERS
  );

  private static final String DEFAULT_ADMIN_NAME = "admin";
  private static final String DEFAULT_SYSTEM_USER_NAME = "druid_system";

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final UserCredentialsMapper credsMapper;
  private final Injector injector;

  @Inject
  public SQLBasicAuthenticatorStorageConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Injector injector
  )
  {
    this.config = config;
    this.injector = injector;
    this.credsMapper = new UserCredentialsMapper();
    this.shouldRetry = new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        return isTransientException(e);
      }
    };
  }

  @LifecycleStart
  public void start()
  {
    AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();

        retryTransaction(
            new TransactionCallback<Void>()
            {
              @Override
              public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
              {
                if (tableExists(handle, getPrefixedTableName(dbConfig.getDbPrefix(), USERS))) {
                  return null;
                }

                createUserTable(dbConfig.getDbPrefix());
                createUserCredentialsTable(dbConfig.getDbPrefix());

                makeDefaultSuperuser(
                    dbConfig.getDbPrefix(),
                    DEFAULT_ADMIN_NAME,
                    dbConfig.getInitialAdminPassword()
                );

                makeDefaultSuperuser(
                    dbConfig.getDbPrefix(),
                    DEFAULT_SYSTEM_USER_NAME,
                    dbConfig.getInitialInternalClientPassword()
                );

                return null;
              }
            },
            3,
            10
        );
      }
    }
  }

  @Override
  public void createUserTable(String dbPrefix)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);
    createTable(
        userTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name),\n"
                + "  UNIQUE (name)\n"
                + ")",
                userTableName
            )
        )
    );
  }

  @Override
  public void createUserCredentialsTable(String dbPrefix)
  {
    final String credentialsTableName = getPrefixedTableName(dbPrefix, USER_CREDENTIALS);
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    createTable(
        credentialsTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  user_name VARCHAR(255) NOT NULL, \n"
                + "  salt VARBINARY(32) NOT NULL, \n"
                + "  hash VARBINARY(64) NOT NULL, \n"
                + "  iterations INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (user_name) REFERENCES %2$s(name) ON DELETE CASCADE\n"
                + ")",
                credentialsTableName,
                userTableName
            )
        )
    );
  }

  @Override
  public void createUser(String dbPrefix, String userName)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getUserCount(handle, dbPrefix, userName);
            if (count != 0) {
              throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
            }

            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (name) VALUES (:user_name)", userTableName
                )
            )
                  .bind("user_name", userName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void deleteUser(String dbPrefix, String userName)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getUserCount(handle, dbPrefix, userName);
            if (count == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE name = :userName", userTableName
                )
            )
                  .bind("userName", userName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getAllUsers(String dbPrefix)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s", userTableName)
                )
                .list();
          }
        }
    );
  }

  @Override
  public Map<String, Object> getUser(String dbPrefix, String userName)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s where name = :userName", userTableName)
                )
                .bind("userName", userName)
                .first();
          }
        }
    );
  }

  @Override
  public Map<String, Object> getUserCredentials(String dbPrefix, String userName)
  {
    final String credentialsTableName = getPrefixedTableName(dbPrefix, USER_CREDENTIALS);

    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCount(handle, dbPrefix, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s where user_name = :userName", credentialsTableName)
                )
                .map(credsMapper)
                .bind("userName", userName)
                .first();
          }
        }
    );
  }

  @Override
  public void setUserCredentials(String dbPrefix, String userName, char[] password)
  {
    final String credentialsTableName = getPrefixedTableName(dbPrefix, USER_CREDENTIALS);

    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCount(handle, dbPrefix, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            Map<String, Object> existingMapping = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT user_name FROM %1$s WHERE user_name = :userName",
                        credentialsTableName
                    )
                )
                .bind("userName", userName)
                .first();

            int iterations = BasicAuthUtils.KEY_ITERATIONS;
            byte[] salt = BasicAuthUtils.generateSalt();
            byte[] hash = BasicAuthUtils.hashPassword(password, salt, iterations);

            if (existingMapping == null) {
              handle.createStatement(
                  StringUtils.format(
                      "INSERT INTO %1$s (user_name, salt, hash, iterations) " +
                      "VALUES (:userName, :salt, :hash, :iterations)",
                      credentialsTableName
                  )
              )
                    .bind("userName", userName)
                    .bind("salt", salt)
                    .bind("hash", hash)
                    .bind("iterations", iterations)
                    .execute();
            } else {
              handle.createStatement(
                  StringUtils.format(
                      "UPDATE %1$s SET " +
                      "salt = :salt, " +
                      "hash = :hash, " +
                      "iterations = :iterations " +
                      "WHERE user_name = :userName",
                      credentialsTableName
                  )
              )
                    .bind("userName", userName)
                    .bind("salt", salt)
                    .bind("hash", hash)
                    .bind("iterations", iterations)
                    .execute();
            }

            return null;
          }
        }
    );
  }

  @Override
  public boolean checkCredentials(String dbPrefix, String userName, char[] password)
  {
    final String credentialsTableName = getPrefixedTableName(dbPrefix, USER_CREDENTIALS);

    return getDBI().inTransaction(
        new TransactionCallback<Boolean>()
        {
          @Override
          public Boolean inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            Map<String, Object> credentials = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT * FROM %1$s WHERE user_name = :userName",
                        credentialsTableName
                    )
                )
                .bind("userName", userName)
                .map(credsMapper)
                .first();

            if (credentials == null) {
              return false;
            }

            byte[] dbSalt = (byte[]) credentials.get("salt");
            byte[] dbHash = (byte[]) credentials.get("hash");
            int iterations = (int) credentials.get("iterations");

            byte[] hash = BasicAuthUtils.hashPassword(password, dbSalt, iterations);

            return Arrays.equals(dbHash, hash);
          }
        }
    );
  }

  public List<String> getTableNamesForPrefix(String dbPrefix)
  {
    return ImmutableList.of(
        getPrefixedTableName(dbPrefix, USER_CREDENTIALS),
        getPrefixedTableName(dbPrefix, USERS)
    );
  }

  public MetadataStorageConnectorConfig getConfig()
  {
    return config.get();
  }

  public String getValidationQuery()
  {
    return "SELECT 1";
  }

  protected BasicDataSource getDatasource()
  {
    MetadataStorageConnectorConfig connectorConfig = getConfig();

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);

    dataSource.setValidationQuery(getValidationQuery());
    dataSource.setTestOnBorrow(true);

    return dataSource;
  }

  protected static String getPrefixedTableName(String dbPrefix, String baseTableName)
  {
    return StringUtils.format("basic_authentication_%s_%s", dbPrefix, baseTableName);
  }

  private void makeDefaultSuperuser(String dbPrefix, String username, String password)
  {
    if (getUser(dbPrefix, username) != null) {
      return;
    }

    createUser(dbPrefix, username);
    setUserCredentials(dbPrefix, username, password.toCharArray());
  }

  private int getUserCount(Handle handle, String dbPrefix, String userName)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", userTableName, "name")
        )
        .bind("key", userName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private static class UserCredentialsMapper implements ResultSetMapper<Map<String, Object>>
  {
    @Override
    public Map<String, Object> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      String user_name = resultSet.getString("user_name");
      byte[] salt = resultSet.getBytes("salt");
      byte[] hash = resultSet.getBytes("hash");
      int iterations = resultSet.getInt("iterations");
      return ImmutableMap.of(
          "user_name", user_name,
          "salt", salt,
          "hash", hash,
          "iterations", iterations
      );
    }
  }
}
