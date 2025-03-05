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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.ranger.authorizer.guice.Ranger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

@JsonTypeName("ranger")
public class RangerAuthorizer implements Authorizer
{
  private static final Logger log = new Logger(RangerAuthorizer.class);

  public static final String RANGER_DRUID_SERVICETYPE = "druid";
  public static final String RANGER_DRUID_APPID = "druid";

  private final RangerBasePlugin rangerPlugin;
  private final boolean useUgi;

  @JsonCreator
  public RangerAuthorizer(
      @JsonProperty("keytab") String keytab,
      @JsonProperty("principal") String principal,
      @JsonProperty("use_ugi") boolean useUgi,
      @JacksonInject @Ranger Configuration conf)
  {
    this.useUgi = useUgi;

    UserGroupInformation.setConfiguration(conf);

    if (keytab != null && principal != null) {
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    rangerPlugin = new RangerBasePlugin(RANGER_DRUID_SERVICETYPE, RANGER_DRUID_APPID);
    rangerPlugin.init();
    rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());

  }

  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    if (authenticationResult == null) {
      throw new IAE("authenticationResult is null where it should never be.");
    }

    Set<String> userGroups = null;
    if (useUgi) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(authenticationResult.getIdentity());
      String[] groups = ugi != null ? ugi.getGroupNames() : null;
      if (groups != null && groups.length > 0) {
        userGroups = new HashSet<>(Arrays.asList(groups));
      }
    }

    RangerDruidResource rangerDruidResource = new RangerDruidResource(resource);
    RangerDruidAccessRequest request = new RangerDruidAccessRequest(
        rangerDruidResource,
        authenticationResult.getIdentity(),
        userGroups,
        action
    );

    RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
    if (log.isDebugEnabled()) {
      log.debug("==> authorize: %s, allowed: %s",
          request.toString(),
          result != null ? result.getIsAllowed() : null);
    }

    if (result != null && result.getIsAllowed()) {
      return new Access(true);
    }

    return new Access(false);
  }
}

class RangerDruidResource extends RangerAccessResourceImpl
{
  public RangerDruidResource(Resource resource)
  {
    setValue(resource.getType().toLowerCase(Locale.ENGLISH), resource.getName());
  }
}

class RangerDruidAccessRequest extends RangerAccessRequestImpl
{
  public RangerDruidAccessRequest(RangerDruidResource resource, String user, Set<String> userGroups, Action action)
  {
    super(resource, action.name().toLowerCase(Locale.ENGLISH), user, userGroups, null);
    setAccessTime(new Date());
  }
}
