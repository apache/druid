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

package org.apache.druid.testsEx.auth;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.storage.local.LocalFileExportStorageProvider;
import org.apache.druid.storage.s3.output.S3ExportStorageProvider;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.clients.SecurityClient;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.categories.Security;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@RunWith(DruidTestRunner.class)
@Category(Security.class)
public class ITSecurityBasicQuery
{
  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;
  @Inject
  private SecurityClient securityClient;

  public static final String USER_1 = "user1";
  public static final String ROLE_1 = "role1";
  public static final String USER_1_PASSWORD = "password1";

  @Before
  public void setUp() throws IOException
  {
    // Authentication setup
    securityClient.createAuthenticationUser(USER_1);
    securityClient.setUserPassword(USER_1, USER_1_PASSWORD);

    // Authorizer setup
    securityClient.createAuthorizerUser(USER_1);
    securityClient.createAuthorizerRole(ROLE_1);
    securityClient.assignUserToRole(USER_1, ROLE_1);
  }

  @After
  public void tearDown() throws Exception
  {
    securityClient.deleteAuthenticationUser(USER_1);
    securityClient.deleteAuthorizerUser(USER_1);
    securityClient.deleteAuthorizerRole(ROLE_1);
  }

  @Test
  public void testIngestionWithoutPermissions() throws Exception
  {
    List<ResourceAction> permissions = ImmutableList.of();
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n",
            "dst"
        );

    // Submit the task and wait for the datasource to get loaded
    StatusResponseHolder statusResponseHolder = msqHelper.submitMsqTask(
        new SqlQuery(queryLocal, null, false, false, false, ImmutableMap.of(), ImmutableList.of()),
        USER_1,
        USER_1_PASSWORD
    );

    Assert.assertEquals(HttpResponseStatus.FORBIDDEN, statusResponseHolder.getStatus());
  }

  @Test
  public void testIngestionWithPermissions() throws Exception
  {
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.READ),
        new ResourceAction(new Resource("EXTERNAL", "EXTERNAL"), Action.READ),
        new ResourceAction(new Resource("STATE", "STATE"), Action.READ),
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.WRITE)
    );
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n",
            "dst"
        );

    // Submit the task and wait for the datasource to get loaded
    StatusResponseHolder statusResponseHolder = msqHelper.submitMsqTask(
        new SqlQuery(queryLocal, null, false, false, false, ImmutableMap.of(), ImmutableList.of()),
        USER_1,
        USER_1_PASSWORD
    );

    Assert.assertEquals(HttpResponseStatus.ACCEPTED, statusResponseHolder.getStatus());
  }

  @Test
  public void testExportWithoutPermissions() throws IOException, ExecutionException, InterruptedException
  {
    // No external write permissions for s3
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.READ),
        new ResourceAction(new Resource("EXTERNAL", "EXTERNAL"), Action.READ),
        new ResourceAction(new Resource(S3ExportStorageProvider.TYPE_NAME, "EXTERNAL"), Action.WRITE),
        new ResourceAction(new Resource("STATE", "STATE"), Action.READ),
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.WRITE)
    );
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String exportQuery =
        StringUtils.format(
            "INSERT INTO extern(%s(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            LocalFileExportStorageProvider.TYPE_NAME, "/shared/export/"
        );

    StatusResponseHolder statusResponseHolder = msqHelper.submitMsqTask(
        new SqlQuery(exportQuery, null, false, false, false, ImmutableMap.of(), ImmutableList.of()),
        USER_1,
        USER_1_PASSWORD
    );

    Assert.assertEquals(HttpResponseStatus.FORBIDDEN, statusResponseHolder.getStatus());
  }

  @Test
  public void testExportWithPermissions() throws IOException, ExecutionException, InterruptedException
  {
    // No external write permissions for s3
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.READ),
        new ResourceAction(new Resource("EXTERNAL", "EXTERNAL"), Action.READ),
        new ResourceAction(new Resource(LocalFileExportStorageProvider.TYPE_NAME, "EXTERNAL"), Action.WRITE),
        new ResourceAction(new Resource("STATE", "STATE"), Action.READ),
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.WRITE)
    );
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String exportQuery =
        StringUtils.format(
            "INSERT INTO extern(%s(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            LocalFileExportStorageProvider.TYPE_NAME, "/shared/export/"
        );

    StatusResponseHolder statusResponseHolder = msqHelper.submitMsqTask(
        new SqlQuery(exportQuery, null, false, false, false, ImmutableMap.of(), ImmutableList.of()),
        USER_1,
        USER_1_PASSWORD
    );

    Assert.assertEquals(HttpResponseStatus.ACCEPTED, statusResponseHolder.getStatus());
  }
}
