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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.server.security.AuthenticationResult;

import javax.naming.InvalidNameException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

@JsonTypeName("ldap")
public class LDAPRoleProvider implements RoleProvider
{
  private static final Logger LOG = new Logger(LDAPRoleProvider.class);

  private final BasicAuthorizerCacheManager cacheManager;
  private final String[] groupFilters;

  @JsonCreator
  public LDAPRoleProvider(
      @JacksonInject BasicAuthorizerCacheManager cacheManager,
      @JsonProperty("groupFilters") String[] groupFilters
  )
  {
    this.cacheManager = cacheManager;
    this.groupFilters = groupFilters;
  }

  @Override
  public Set<String> getRoles(String authorizerPrefix, AuthenticationResult authenticationResult)
  {
    Set<String> roleNames = new HashSet<>();
    Map<String, BasicAuthorizerGroupMapping> groupMappingMap = cacheManager.getGroupMappingMap(authorizerPrefix);
    if (groupMappingMap == null) {
      throw new IAE("Could not load groupMappingMap for authorizer [%s]", authorizerPrefix);
    }
    Map<String, BasicAuthorizerUser> userMap = cacheManager.getUserMap(authorizerPrefix);
    if (userMap == null) {
      throw new IAE("Could not load userMap for authorizer [%s]", authorizerPrefix);
    }

    // Get the groups assigned to the LDAP user
    SearchResult searchResult = Optional.ofNullable(authenticationResult.getContext())
                                               .map(contextMap -> contextMap.get(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY))
                                               .map(p -> {
                                                 if (p instanceof SearchResult) {
                                                   return (SearchResult) p;
                                                 } else {
                                                   return null;
                                                 }
                                               })
                                               .orElse(null);
    if (searchResult != null) {
      try {
        Set<LdapName> groupNamesFromLdap = getGroupsFromLdap(searchResult);
        if (groupNamesFromLdap.isEmpty()) {
          LOG.debug("User %s is not mapped to any groups", authenticationResult.getIdentity());
        } else {
          // Get the roles mapped to LDAP groups from the metastore.
          // This allows us to authorize groups LDAP user belongs
          roleNames.addAll(getRoles(groupMappingMap, groupNamesFromLdap));
        }
      }
      catch (NamingException e) {
        LOG.error(e, "Exception in looking up groups for user %s", authenticationResult.getIdentity());
      }
    }

    // Get the roles assigned to LDAP user from the metastore.
    // This allow us to authorize LDAP users regardless of whether they belong to any groups or not in LDAP.  
    BasicAuthorizerUser user = userMap.get(authenticationResult.getIdentity());
    if (user != null) {
      roleNames.addAll(user.getRoles());
    }

    return roleNames;
  }

  @Override
  public Map<String, BasicAuthorizerRole> getRoleMap(String authorizerPrefix)
  {
    return cacheManager.getRoleMap(authorizerPrefix);
  }

  @VisibleForTesting
  public Set<String> getRoles(Map<String, BasicAuthorizerGroupMapping> groupMappingMap, Set<LdapName> groupNamesFromLdap)
  {
    Set<String> roles = new HashSet<>();

    if (groupMappingMap.size() == 0) {
      return roles;
    }

    for (LdapName groupName : groupNamesFromLdap) {
      for (Map.Entry<String, BasicAuthorizerGroupMapping> groupMappingEntry : groupMappingMap.entrySet()) {
        BasicAuthorizerGroupMapping groupMapping = groupMappingEntry.getValue();
        String mask = groupMapping.getGroupPattern();
        try {
          if (mask.startsWith("*,")) {
            LdapName ln = new LdapName(mask.substring(2));
            if (groupName.startsWith(ln)) {
              roles.addAll(groupMapping.getRoles());
            }
          } else if (mask.endsWith(",*")) {
            LdapName ln = new LdapName(mask.substring(0, mask.length() - 2));
            if (groupName.endsWith(ln)) {
              roles.addAll(groupMapping.getRoles());
            }
          } else {
            LdapName ln = new LdapName(mask);
            if (groupName.equals(ln)) {
              roles.addAll(groupMapping.getRoles());
            }
          }
        }
        catch (InvalidNameException e) {
          throw new RuntimeException(String.format(Locale.getDefault(),
                                                   "Configuration problem - Invalid groupMapping '%s'", mask));
        }
      }
    }
    return roles;
  }

  Set<LdapName> getGroupsFromLdap(SearchResult userResult) throws NamingException
  {
    Set<LdapName> groups = new TreeSet<>();

    Attribute memberOf = userResult.getAttributes().get("memberOf");
    if (memberOf == null) {
      LOG.debug("No memberOf attributes");
      return groups; // not part of any groups
    }

    for (int i = 0; i < memberOf.size(); i++) {
      String memberDn = memberOf.get(i).toString();
      LdapName ln;
      try {
        ln = new LdapName(memberDn);
      }
      catch (InvalidNameException e) {
        LOG.debug("Invalid LDAP name: %s", memberDn);
        continue;
      }
      if (this.groupFilters != null) {
        if (allowedLdapGroup(ln, new TreeSet<>(Arrays.asList(this.groupFilters)))) {
          groups.add(ln);
        }
      } else {
        groups.add(ln);
      }
    }
    return groups;
  }

  boolean allowedLdapGroup(LdapName groupName, Set<String> groupFilters)
  {
    for (String filter : groupFilters) {
      try {
        if (filter.startsWith("*,")) {
          LdapName ln = new LdapName(filter.substring(2));
          if (groupName.startsWith(ln)) {
            return true;
          }
        } else if (filter.endsWith(",*")) {
          LdapName ln = new LdapName(filter.substring(0, filter.length() - 2));
          if (groupName.endsWith(ln)) {
            return true;
          }
        } else {
          LOG.debug("Attempting exact filter %s", filter);
          LdapName ln = new LdapName(filter);
          if (groupName.equals(ln)) {
            return true;
          }
        }
      }
      catch (InvalidNameException e) {
        throw new RE(StringUtils.format("Configuration problem - Invalid groupFilter '%s'", filter));
      }
    }
    return false;
  }
}
