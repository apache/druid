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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.metadata.BaseSQLMetadataConnector;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.BasicAuthConfig;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Base class for BasicSecurityStorageConnector implementations that interface with a database using SQL.
 */
public abstract class SQLBasicSecurityStorageConnector
    extends BaseSQLMetadataConnector
    implements BasicSecurityStorageConnector
{
  public static final String USERS = "users";
  public static final String USER_CREDENTIALS = "user_credentials";
  public static final String PERMISSIONS = "permissions";
  public static final String ROLES = "roles";
  public static final String USER_ROLES = "user_roles";

  public static final List<String> TABLE_NAMES = Lists.newArrayList(
      USER_CREDENTIALS,
      USER_ROLES,
      PERMISSIONS,
      ROLES,
      USERS
  );

  private static final String DEFAULT_ADMIN_NAME = "admin";
  private static final String DEFAULT_ADMIN_ROLE = "admin";

  private static final String DEFAULT_SYSTEM_USER_NAME = "druid_system";
  private static final String DEFAULT_SYSTEM_USER_ROLE = "druid_system";

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final BasicAuthConfig basicAuthConfig;
  private final ObjectMapper jsonMapper;
  private final PermissionsMapper permMapper;
  private final UserCredentialsMapper credsMapper;

  @Inject
  public SQLBasicSecurityStorageConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<BasicAuthConfig> basicAuthConfigSupplier,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.basicAuthConfig = basicAuthConfigSupplier.get();
    this.jsonMapper = jsonMapper;
    this.permMapper = new PermissionsMapper();
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
    createUserTable();
    createRoleTable();
    createPermissionTable();
    createUserRoleTable();
    createUserCredentialsTable();

    makeDefaultSuperuser(DEFAULT_ADMIN_NAME, basicAuthConfig.getInitialAdminPassword(), DEFAULT_ADMIN_ROLE);
    makeDefaultSuperuser(DEFAULT_SYSTEM_USER_NAME, basicAuthConfig.getInitialInternalClientPassword(), DEFAULT_SYSTEM_USER_ROLE);
  }

  @Override
  public void createRoleTable()
  {
    createTable(
        ROLES,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name),\n"
                + "  UNIQUE (name)\n"
                + ")",
                ROLES
            )
        )
    );
  }

  @Override
  public void createUserTable()
  {
    createTable(
        USERS,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name),\n"
                + "  UNIQUE (name)\n"
                + ")",
                USERS
            )
        )
    );
  }

  @Override
  public void createUserCredentialsTable()
  {
    createTable(
        USER_CREDENTIALS,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  user_name VARCHAR(255) NOT NULL, \n"
                + "  salt VARBINARY(32) NOT NULL, \n"
                + "  hash VARBINARY(64) NOT NULL, \n"
                + "  iterations INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (user_name) REFERENCES users(name) ON DELETE CASCADE\n"
                + ")",
                USER_CREDENTIALS
            )
        )
    );
  }

  @Override
  public void createPermissionTable()
  {
    createTable(
        PERMISSIONS,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id INTEGER NOT NULL,\n"
                + "  resource_json VARCHAR(255) NOT NULL,\n"
                + "  role_name INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (id),\n"
                + "  FOREIGN KEY (role_name) REFERENCES roles(name) ON DELETE CASCADE\n"
                + ")",
                PERMISSIONS
            )
        )
    );
  }

  @Override
  public void createUserRoleTable()
  {
    createTable(
        USER_ROLES,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  user_name VARCHAR(255) NOT NULL,\n"
                + "  role_name VARCHAR(255) NOT NULL, \n"
                + "  FOREIGN KEY (user_name) REFERENCES users(name) ON DELETE CASCADE,\n"
                + "  FOREIGN KEY (role_name) REFERENCES roles(name) ON DELETE CASCADE\n"
                + ")",
                USER_ROLES
            )
        )
    );
  }

  @Override
  public void deleteAllRecords(String tableName)
  {
    throw new UnsupportedOperationException("delete all not supported yet for authorization storage");
  }

  public MetadataStorageConnectorConfig getConfig()
  {
    return config.get();
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

  public String getValidationQuery()
  {
    return "SELECT 1";
  }

  @Override
  public void createUser(String userName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getUserCountInTransaction(handle, userName);
            if (count != 0) {
              throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
            }

            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (name) VALUES (:user_name)", USERS
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
  public void deleteUser(String userName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getUserCountInTransaction(handle, userName);
            if (count == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE name = :userName", USERS
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
  public void createRole(String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getRoleCountInTransaction(handle, roleName);
            if (count != 0) {
              throw new BasicSecurityDBResourceException("Role [%s] already exists.", roleName);
            }
            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (name) VALUES (:roleName)", ROLES
                )
            )
                  .bind("roleName", roleName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void deleteRole(String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getRoleCountInTransaction(handle, roleName);
            if (count == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE name = :roleName", ROLES
                )
            )
                  .bind("roleName", roleName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void addPermission(String roleName, ResourceAction resourceAction)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int roleCount = getRoleCountInTransaction(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            try {
              byte[] serializedResourceAction = jsonMapper.writeValueAsBytes(resourceAction);
              handle.createStatement(
                  StringUtils.format(
                      "INSERT INTO %1$s (resource_json, role_name) VALUES (:resourceJson, :roleName)",
                      PERMISSIONS
                  )
              )
                    .bind("resourceJson", serializedResourceAction)
                    .bind("roleName", roleName)
                    .execute();

              return null;
            }
            catch (JsonProcessingException jpe) {
              throw new IAE(jpe, "Could not serialize resourceAction [%s].", resourceAction);
            }
          }
        }
    );
  }

  @Override
  public void deleteAllPermissionsFromRole(String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int roleCount = getRoleCountInTransaction(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE role_name = :roleName",
                    PERMISSIONS
                )
            )
                  .bind("roleName", roleName)
                  .execute();

            return null;
          }
        }
    );
  }

  @Override
  public void deletePermission(int permissionId)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int permCount = getPermissionCountInTransaction(handle, permissionId);
            if (permCount == 0) {
              throw new BasicSecurityDBResourceException("Permission with id [%s] does not exist.", permissionId);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE id = :permissionId", PERMISSIONS
                )
            )
                  .bind("permissionId", permissionId)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void assignRole(String userName, String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCountInTransaction(handle, userName);
            int roleCount = getRoleCountInTransaction(handle, roleName);

            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            int userRoleMappingCount = getUserRoleMappingCountInTransaction(handle, userName, roleName);
            if (userRoleMappingCount != 0) {
              throw new BasicSecurityDBResourceException("User [%s] already has role [%s].", userName, roleName);
            }

            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (user_name, role_name) VALUES (:userName, :roleName)", USER_ROLES
                )
            )
                  .bind("userName", userName)
                  .bind("roleName", roleName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void unassignRole(String userName, String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCountInTransaction(handle, userName);
            int roleCount = getRoleCountInTransaction(handle, roleName);

            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            int userRoleMappingCount = getUserRoleMappingCountInTransaction(handle, userName, roleName);
            if (userRoleMappingCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not have role [%s].", userName, roleName);
            }

            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE user_name = :userName AND role_name = :roleName", USER_ROLES
                )
            )
                  .bind("userName", userName)
                  .bind("roleName", roleName)
                  .execute();

            return null;
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getAllUsers()
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM users")
                )
                .list();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getAllRoles()
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM roles")
                )
                .list();
          }
        }
    );
  }

  @Override
  public Map<String, Object> getUser(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM users where name = :userName")
                )
                .bind("userName", userName)
                .first();
          }
        }
    );
  }

  @Override
  public Map<String, Object> getRole(String roleName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM roles where name = :roleName")
                )
                .bind("roleName", roleName)
                .first();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getRolesForUser(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int userCount = getUserCountInTransaction(handle, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT roles.name\n"
                        + "FROM roles\n"
                        + "JOIN user_roles\n"
                        + "    ON user_roles.role_name = roles.name\n"
                        + "WHERE user_roles.user_name = :userName"
                    )
                )
                .bind("userName", userName)
                .list();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getUsersWithRole(String roleName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int roleCount = getRoleCountInTransaction(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT users.name\n"
                        + "FROM users\n"
                        + "JOIN user_roles\n"
                        + "    ON user_roles.user_name = users.name\n"
                        + "WHERE user_roles.role_name = :roleName"
                    )
                )
                .bind("roleName", roleName)
                .list();
          }
        }
    );
  }

  private class PermissionsMapper implements ResultSetMapper<Map<String, Object>>
  {
    @Override
    public Map<String, Object> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      int id = resultSet.getInt("id");
      byte[] resourceJson = resultSet.getBytes("resource_json");
      try {
        final ResourceAction resourceAction = jsonMapper.readValue(resourceJson, ResourceAction.class);
        return ImmutableMap.of(
            "id", id,
            "resourceAction", resourceAction
        );
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<Map<String, Object>> getPermissionsForRole(String roleName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int roleCount = getRoleCountInTransaction(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT permissions.id, permissions.resource_json\n"
                        + "FROM permissions\n"
                        + "WHERE permissions.role_name = :roleName"
                    )
                )
                .map(permMapper)
                .bind("roleName", roleName)
                .list();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getPermissionsForUser(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int userCount = getUserCountInTransaction(handle, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT permissions.id, permissions.resource_json, roles.name\n"
                        + "FROM permissions\n"
                        + "JOIN roles\n"
                        + "    ON permissions.role_name = roles.name\n"
                        + "JOIN user_roles\n"
                        + "    ON user_roles.role_name = roles.name\n"
                        + "WHERE user_roles.user_name = :userName"
                    )
                )
                .map(permMapper)
                .bind("userName", userName)
                .list();
          }
        }
    );
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

  @Override
  public Map<String, Object> getUserCredentials(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCountInTransaction(handle, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s where user_name = :userName", USER_CREDENTIALS)
                )
                .map(credsMapper)
                .bind("userName", userName)
                .first();
          }
        }
    );
  }

  @Override
  public void setUserCredentials(String userName, char[] password)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCountInTransaction(handle, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            Map<String, Object> existingMapping = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT user_name FROM %1$s WHERE user_name = :userName",
                        USER_CREDENTIALS
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
                      USER_CREDENTIALS
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
                      USER_CREDENTIALS
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
  public boolean checkCredentials(String userName, char[] password)
  {
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
                        USER_CREDENTIALS
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

  private int getUserCountInTransaction(Handle handle, String userName)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", USERS, "name")
        )
        .bind("key", userName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private int getRoleCountInTransaction(Handle handle, String roleName)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", ROLES, "name")
        )
        .bind("key", roleName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private int getPermissionCountInTransaction(Handle handle, int permissionId)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", PERMISSIONS, "id")
        )
        .bind("key", permissionId)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private int getUserRoleMappingCountInTransaction(Handle handle, String userName, String roleName)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :userkey AND %3$s = :rolekey",
                               USER_ROLES,
                               "user_name",
                               "role_name"
            )
        )
        .bind("userkey", userName)
        .bind("rolekey", roleName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private void makeDefaultSuperuser(String username, String password, String role)
  {
    if (getUser(username) != null) {
      return;
    }

    createUser(username);
    createRole(role);
    assignRole(username, role);

    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    List<ResourceAction> resActs = Lists.newArrayList(datasourceR, datasourceW, configR, configW, stateR, stateW);

    for (ResourceAction resAct : resActs) {
      addPermission(role, resAct);
    }

    setUserCredentials(username, password.toCharArray());
  }
}
