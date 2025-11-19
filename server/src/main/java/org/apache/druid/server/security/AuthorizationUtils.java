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

package org.apache.druid.server.security;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.audit.RequestInfo;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.policy.Policy;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Static utility functions for performing authorization checks.
 */
public class AuthorizationUtils
{
  public static final ImmutableSet<String> RESTRICTION_APPLICABLE_RESOURCE_TYPES = ImmutableSet.of(
      ResourceType.DATASOURCE
  );

  /**
   * Performs authorization check on a single resource-action based on the authentication fields from the request.
   * <p>
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request. If this attribute is already set
   * when this function is called, an exception is thrown.
   *
   * @param request          HTTP request to be authorized
   * @param resourceAction   A resource identifier and the action to be taken the resource.
   * @param authorizerMapper The singleton AuthorizerMapper instance
   * @return AuthorizationResult containing allow/deny access to the resource action, along with policy restrictions.
   */
  public static AuthorizationResult authorizeResourceAction(
      final HttpServletRequest request,
      final ResourceAction resourceAction,
      final AuthorizerMapper authorizerMapper
  )
  {
    return authorizeAllResourceActions(
        request,
        Collections.singletonList(resourceAction),
        authorizerMapper
    );
  }

  /**
   * Verifies that the user has unrestricted access to perform the required
   * action on the given datasource.
   *
   * @throws ForbiddenException if the user does not have unrestricted access to
   * perform the required action on the given datasource.
   */
  public static void verifyUnrestrictedAccessToDatasource(
      final HttpServletRequest req,
      String datasource,
      AuthorizerMapper authorizerMapper
  )
  {
    ResourceAction resourceAction = createDatasourceResourceAction(datasource, req);
    AuthorizationResult authResult = authorizeResourceAction(req, resourceAction, authorizerMapper);
    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
    }
  }

  /**
   * Returns the authentication information for a request.
   *
   * @param request http request
   * @return authentication result
   * @throws IllegalStateException if the request was not authenticated
   */
  public static AuthenticationResult authenticationResultFromRequest(final HttpServletRequest request)
  {
    final AuthenticationResult authenticationResult = (AuthenticationResult) request.getAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT
    );

    if (authenticationResult == null) {
      throw new ISE("Null authentication result");
    }

    return authenticationResult;
  }

  /**
   * Extracts the identity from the authentication result if set as an atrribute
   * of this request.
   */
  @Nullable
  public static String getAuthenticatedIdentity(HttpServletRequest request)
  {
    final AuthenticationResult authenticationResult = (AuthenticationResult) request.getAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT
    );

    if (authenticationResult == null) {
      return null;
    } else {
      return authenticationResult.getIdentity();
    }
  }

  /**
   * Builds an AuditInfo for the given request by extracting the following from
   * it:
   * <ul>
   * <li>Header {@link AuditManager#X_DRUID_AUTHOR}</li>
   * <li>Header {@link AuditManager#X_DRUID_COMMENT}</li>
   * <li>Attribute {@link AuthConfig#DRUID_AUTHENTICATION_RESULT}</li>
   * <li>IP address using {@link HttpServletRequest#getRemoteAddr()}</li>
   * </ul>
   */
  public static AuditInfo buildAuditInfo(HttpServletRequest request)
  {
    final String author = request.getHeader(AuditManager.X_DRUID_AUTHOR);
    final String comment = request.getHeader(AuditManager.X_DRUID_COMMENT);
    return new AuditInfo(
        author == null ? "" : author,
        getAuthenticatedIdentity(request),
        comment == null ? "" : comment,
        request.getRemoteAddr()
    );
  }

  /**
   * Builds a RequestInfo object that can be used for auditing purposes.
   */
  public static RequestInfo buildRequestInfo(String service, HttpServletRequest request)
  {
    return new RequestInfo(
        service,
        request.getMethod(),
        request.getRequestURI(),
        request.getQueryString()
    );
  }

  /**
   * Performs authorization check on a list of resource-actions based on the authenticationResult.
   * <p>
   * If one of the resource-actions denys access, returns deny access immediately.
   *
   * @param authenticationResult Authentication result representing identity of requester
   * @param resourceActions      An Iterable of resource-actions to authorize
   * @return AuthorizationResult containing allow/deny access to the resource actions, along with policy restrictions.
   */
  public static AuthorizationResult authorizeAllResourceActions(
      final AuthenticationResult authenticationResult,
      final Iterable<ResourceAction> resourceActions,
      final AuthorizerMapper authorizerMapper
  )
  {
    final Authorizer authorizer = authorizerMapper.getAuthorizer(authenticationResult.getAuthorizerName());
    if (authorizer == null) {
      throw new ISE("No authorizer found with name: [%s].", authenticationResult.getAuthorizerName());
    }

    // this method returns on first failure, so only successful Access results are kept in the cache
    final Set<ResourceAction> resultCache = new HashSet<>();
    final Map<String, Optional<Policy>> policyFilters = new HashMap<>();

    for (ResourceAction resourceAction : resourceActions) {
      if (resultCache.contains(resourceAction)) {
        continue;
      }
      final Access access = authorizer.authorize(
          authenticationResult,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        return AuthorizationResult.deny(access.getMessage());
      } else {
        resultCache.add(resourceAction);
        if (shouldApplyPolicy(resourceAction.getResource(), resourceAction.getAction())) {
          // For every table read, we check on the policy returned from authorizer and add it to the map.
          policyFilters.put(resourceAction.getResource().getName(), access.getPolicy());
        } else if (access.getPolicy().isPresent()) {
          throw DruidException.defensive(
              "Policy should only present when reading a table, but was present for a different kind of resource action [%s]",
              resourceAction
          );
        } else {
          // Not a read table action, access doesn't have a filter, do nothing.
        }
      }
    }

    return AuthorizationResult.allowWithRestriction(policyFilters);
  }

  public static boolean shouldApplyPolicy(Resource resource, Action action)
  {
    return Action.READ.equals(action) || RESTRICTION_APPLICABLE_RESOURCE_TYPES.contains(resource.getType());
  }


  /**
   * Performs authorization check on a list of resource-actions based on the authentication fields from the request.
   * <p>
   * If one of the resource-actions denys access, returns deny access immediately.
   * <p>
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request. If this attribute is already set
   * when this function is called, an exception is thrown.
   *
   * @param request         HTTP request to be authorized
   * @param resourceActions An Iterable of resource-actions to authorize
   * @return AuthorizationResult containing allow/deny access to the resource actions, along with policy restrictions.
   */
  public static AuthorizationResult authorizeAllResourceActions(
      final HttpServletRequest request,
      final Iterable<ResourceAction> resourceActions,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
      return AuthorizationResult.ALLOW_NO_RESTRICTION;
    }

    if (request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED) != null) {
      throw new ISE("Request already had authorization check.");
    }

    AuthorizationResult authResult = authorizeAllResourceActions(
        authenticationResultFromRequest(request),
        resourceActions,
        authorizerMapper
    );

    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, authResult.allowBasicAccess());
    return authResult;
  }

  /**
   * Sets the {@link AuthConfig#DRUID_AUTHORIZATION_CHECKED} attribute in the {@link HttpServletRequest} to true. This method is generally used
   * when no {@link ResourceAction} need to be checked for the API. If resources are present, users should call
   * {@link AuthorizationUtils#authorizeAllResourceActions(HttpServletRequest, Iterable, AuthorizerMapper)}
   */
  public static void setRequestAuthorizationAttributeIfNeeded(final HttpServletRequest request)
  {
    if (request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
      // do nothing since request allows unsecured paths to proceed. Generally, this is used for custom urls.
      return;
    }
    if (request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED) != null) {
      throw DruidException.defensive("Request already had authorization check.");
    }
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
  }

  /**
   * Return an iterable of authorized resources, by filtering the input resources with authorization checks based on the
   * authentication fields from the request. This method does:
   * <li>
   * For every resource, resourceActionGenerator generates an Iterable of ResourceAction or null.
   * <li>
   * If null, continue with next resource. If any resource-action in the iterable has deny-access, continue with next
   * resource. Only when every resource-action has allow-access, add the resource to the result.
   * </li>
   * <p>
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request. If this attribute is already set
   * when this function is called, an exception is thrown.
   *
   * @param request                 HTTP request to be authorized
   * @param resources               resources to be processed into resource-actions
   * @param resourceActionGenerator Function that creates an iterable of resource-actions from a resource
   * @param authorizerMapper        authorizer mapper
   * @return Iterable containing resources that were authorized
   */
  public static <ResType> Iterable<ResType> filterAuthorizedResources(
      final HttpServletRequest request,
      final Iterable<ResType> resources,
      final Function<? super ResType, Iterable<ResourceAction>> resourceActionGenerator,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
      return resources;
    }

    if (request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED) != null) {
      throw new ISE("Request already had authorization check.");
    }

    final AuthenticationResult authenticationResult = authenticationResultFromRequest(request);

    final Iterable<ResType> filteredResources = filterAuthorizedResources(
        authenticationResult,
        resources,
        resourceActionGenerator,
        authorizerMapper
    );

    // We're filtering, so having access to none of the objects isn't an authorization failure (in terms of whether
    // to send an error response or not.)
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    return filteredResources;
  }

  /**
   * Return an iterable of authorized resources, by filtering the input resources with authorization checks based on
   * authenticationResult. This method does:
   * <li>
   * For every resource, resourceActionGenerator generates an Iterable of ResourceAction or null.
   * <li>
   * If null, continue with next resource. If any resource-action in the iterable has deny-access, continue with next
   * resource. Only when every resource-action has allow-access, add the resource to the result.
   *
   * @param authenticationResult    Authentication result representing identity of requester
   * @param resources               resources to be processed into resource-actions
   * @param resourceActionGenerator Function that creates an iterable of resource-actions from a resource
   * @param authorizerMapper        authorizer mapper
   * @return Iterable containing resources that were authorized
   */
  public static <ResType> Iterable<ResType> filterAuthorizedResources(
      final AuthenticationResult authenticationResult,
      final Iterable<ResType> resources,
      final Function<? super ResType, Iterable<ResourceAction>> resourceActionGenerator,
      final AuthorizerMapper authorizerMapper
  )
  {
    final Authorizer authorizer = authorizerMapper.getAuthorizer(authenticationResult.getAuthorizerName());
    if (authorizer == null) {
      throw new ISE("No authorizer found with name: [%s].", authenticationResult.getAuthorizerName());
    }

    final Map<ResourceAction, Access> resultCache = new HashMap<>();
    return Iterables.filter(
        resources,
        resource -> {
          final Iterable<ResourceAction> resourceActions = resourceActionGenerator.apply(resource);
          if (resourceActions == null) {
            return false;
          }
          if (authorizer instanceof AllowAllAuthorizer) {
            return true;
          }
          for (ResourceAction resourceAction : resourceActions) {
            Access access = resultCache.computeIfAbsent(
                resourceAction,
                ra -> authorizer.authorize(
                    authenticationResult,
                    ra.getResource(),
                    ra.getAction()
                )
            );
            if (!access.isAllowed()) {
              return false;
            }
          }
          return true;
        }
    );
  }

  /**
   * Return a map of authorized resources, by filtering the input resources with authorization checks based on the
   * authentication fields from the request. This method does:
   * <li>
   * For every resource, resourceActionGenerator generates an Iterable of ResourceAction or null.
   * <li>
   * If null, continue with next resource. If any resource-action in the iterable has deny-access, continue with next
   * resource. Only when every resource-action has allow-access, add the resource to the result.
   * </li>
   * <p>
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request. If this attribute is already set
   * when this function is called, an exception is thrown.
   *
   * @param request                 HTTP request to be authorized
   * @param unfilteredResources     Map of resource lists to be filtered
   * @param resourceActionGenerator Function that creates an iterable of resource-actions from a resource
   * @param authorizerMapper        authorizer mapper
   * @return Map containing lists of resources that were authorized
   */
  public static <KeyType, ResType> Map<KeyType, List<ResType>> filterAuthorizedResources(
      final HttpServletRequest request,
      final Map<KeyType, List<ResType>> unfilteredResources,
      final Function<? super ResType, Iterable<ResourceAction>> resourceActionGenerator,
      final AuthorizerMapper authorizerMapper
  )
  {

    if (request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
      return unfilteredResources;
    }

    if (request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED) != null) {
      throw new ISE("Request already had authorization check.");
    }

    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(request);

    Map<KeyType, List<ResType>> filteredResources = new HashMap<>();
    for (Map.Entry<KeyType, List<ResType>> entry : unfilteredResources.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }

      final List<ResType> filteredList = Lists.newArrayList(
          AuthorizationUtils.filterAuthorizedResources(
              authenticationResult,
              entry.getValue(),
              resourceActionGenerator,
              authorizerMapper
          )
      );

      if (!filteredList.isEmpty()) {
        filteredResources.put(
            entry.getKey(),
            filteredList
        );
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    return filteredResources;
  }

  /**
   * Filters the given datasource-related resources on the basis of datasource
   * permissions.
   *
   * @return List of resources to which the user has access, based on whether
   * the user has access to the underlying datasource or not.
   */
  public static <T> List<T> filterByAuthorizedDatasources(
      final HttpServletRequest request,
      List<T> resources,
      Function<T, String> getDatasource,
      AuthorizerMapper authorizerMapper
  )
  {
    final Function<T, Iterable<ResourceAction>> raGenerator =
        entry -> List.of(createDatasourceResourceAction(getDatasource.apply(entry), request));

    return Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            request,
            resources,
            raGenerator,
            authorizerMapper
        )
    );
  }

  private static ResourceAction createDatasourceResourceAction(
      String dataSource,
      HttpServletRequest request
  )
  {
    switch (request.getMethod()) {
      case "GET":
      case "HEAD":
        return AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(dataSource);
      default:
        return AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR.apply(dataSource);
    }
  }

  /**
   * Creates a {@link ResourceAction} to {@link Action#READ read} an
   * {@link ResourceType#EXTERNAL external} resource.
   */
  public static ResourceAction createExternalResourceReadAction(String resourceName)
  {
    return new ResourceAction(
        new Resource(resourceName, ResourceType.EXTERNAL),
        Action.READ
    );
  }

  /**
   * This method constructs a 'superuser' set of permissions composed of {@link Action#READ} and {@link Action#WRITE}
   * permissions for all known {@link ResourceType#knownTypes()} for any {@link Authorizer} implementation which is
   * built on pattern matching with a regex.
   * <p>
   * Note that if any {@link Resource} exist that use custom types not registered with
   * {@link ResourceType#registerResourceType}, those permissions will not be included in this list and will need to
   * be added manually.
   */
  public static List<ResourceAction> makeSuperUserPermissions()
  {
    List<ResourceAction> allReadAndWrite = new ArrayList<>(ResourceType.knownTypes().size() * 2);
    for (String type : ResourceType.knownTypes()) {
      allReadAndWrite.add(
          new ResourceAction(new Resource(".*", type), Action.READ)
      );
      allReadAndWrite.add(
          new ResourceAction(new Resource(".*", type), Action.WRITE)
      );
    }
    return allReadAndWrite;
  }

  /**
   * Function for the common pattern of generating a resource-action for reading from a datasource, using the
   * datasource name.
   */
  public static final Function<String, ResourceAction> DATASOURCE_READ_RA_GENERATOR = input -> new ResourceAction(
      new Resource(input, ResourceType.DATASOURCE),
      Action.READ
  );

  /**
   * Function for the common pattern of generating a resource-action for reading from a datasource, using the
   * datasource name.
   */
  public static final Function<String, ResourceAction> DATASOURCE_WRITE_RA_GENERATOR = input -> new ResourceAction(
      new Resource(input, ResourceType.DATASOURCE),
      Action.WRITE
  );

}
