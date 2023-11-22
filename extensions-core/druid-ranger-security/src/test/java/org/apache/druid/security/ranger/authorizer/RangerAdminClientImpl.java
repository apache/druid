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

package org.apache.druid.security.ranger.authorizer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.AbstractRangerAdminClient;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class RangerAdminClientImpl extends AbstractRangerAdminClient
{
  private static final Logger LOG = new Logger(RangerAdminClientImpl.class);
  private static final String CACHE_FILE_NAME = "druid-policies.json";

  protected Gson gson;

  @Override
  public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config)
  {
    super.init(serviceName, appId, configPropertyPrefix, config);

    try {
      gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
    }
    catch (Throwable excp) {
      LOG.error(excp, "AbstractRangerAdminClient: failed to create GsonBuilder object");
    }
  }

  @Override
  public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis)
      throws Exception
  {

    String basedir = System.getProperty("basedir");
    if (basedir == null) {
      basedir = new File(".").getCanonicalPath();
    }

    Path cachePath = FileSystems.getDefault().getPath(basedir, "/src/test/resources/" + CACHE_FILE_NAME);
    byte[] cacheBytes = Files.readAllBytes(cachePath);

    return gson.fromJson(new String(cacheBytes, StandardCharsets.UTF_8), ServicePolicies.class);
  }

}
