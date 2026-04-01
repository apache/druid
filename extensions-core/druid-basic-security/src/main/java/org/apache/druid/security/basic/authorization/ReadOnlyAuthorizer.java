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

package org.apache.druid.security.basic.authorization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import javax.annotation.Nullable;

/**
 * An authorizer that allows all READ operations and denies all other operations (WRITE, DELETE, etc.).
 */
@JsonTypeName("readonly")
public class ReadOnlyAuthorizer implements Authorizer
{
  private static final Logger LOG = new Logger(ReadOnlyAuthorizer.class);

  @Nullable
  private final Policy policy;

  @JsonCreator
  public ReadOnlyAuthorizer(
      @JsonProperty("policy") @Nullable Policy policy
  )
  {
    this.policy = policy;
  }

  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    if (authenticationResult == null) {
      throw new IAE("authenticationResult is null where it should never be.");
    }
    if (action == Action.READ) {
      if (shouldApplyPolicy(resource, action)) {
        return Access.allowWithRestriction(policy);
      }
      return Access.OK;
    }
    LOG.info("Authorization failed for user=%s on action=%s, %s", authenticationResult.getIdentity(), action, resource);
    return Access.deny(null);
  }

  private boolean shouldApplyPolicy(Resource resource, Action action)
  {
    return policy != null && AuthorizationUtils.shouldApplyPolicy(resource, action);
  }
}
