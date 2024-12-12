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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.audit.RequestInfo;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Static utility functions for performing authorization checks.
 */
public class AuthorizationUtils
{
  /**
   * Check a resource-action using the authorization fields from the request.
   *
   * Otherwise, if the resource-actions is authorized, return ACCESS_OK.
   *
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request          HTTP request to be authorized
   * @param resourceAction   A resource identifier and the action to be taken the resource.
   * @param authorizerMapper The singleton AuthorizerMapper instance
   *
   * @return ACCESS_OK or the failed Access object returned by the Authorizer that checked the request.
   */
  public static Access authorizeResourceAction(
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
   * Returns the authentication information for a request.
   *
   * @param request http request
   *
   * @return authentication result
   *
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
   * Check a list of resource-actions to be performed by the identity represented by authenticationResult.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * @param authenticationResult Authentication result representing identity of requester
   * @param resourceActions      An Iterable of resource-actions to authorize
   *
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static Access authorizeAllResourceActions(
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
        return access;
      } else {
        resultCache.add(resourceAction);
      }
    }

    return Access.OK;
  }

  /**
   * Check a list of resource-actions to be performed as a result of an HTTP request.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request         HTTP request to be authorized
   * @param resourceActions An Iterable of resource-actions to authorize
   *
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static Access authorizeAllResourceActions(
      final HttpServletRequest request,
      final Iterable<ResourceAction> resourceActions,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
      return Access.OK;
    }

    if (request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED) != null) {
      throw new ISE("Request already had authorization check.");
    }

    Access access = authorizeAllResourceActions(
        authenticationResultFromRequest(request),
        resourceActions,
        authorizerMapper
    );

    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, access.isAllowed());
    return access;
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
   * Filter a collection of resources by applying the resourceActionGenerator to each resource, return an iterable
   * containing the filtered resources.
   *
   * The resourceActionGenerator returns an Iterable<ResourceAction> for each resource.
   *
   * If every resource-action in the iterable is authorized, the resource will be added to the filtered resources.
   *
   * If there is an authorization failure for one of the resource-actions, the resource will not be
   * added to the returned filtered resources..
   *
   * If the resourceActionGenerator returns null for a resource, that resource will not be added to the filtered
   * resources.
   *
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request                 HTTP request to be authorized
   * @param resources               resources to be processed into resource-actions
   * @param resourceActionGenerator Function that creates an iterable of resource-actions from a resource
   * @param authorizerMapper        authorizer mapper
   *
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
   * Filter a collection of resources by applying the resourceActionGenerator to each resource, return an iterable
   * containing the filtered resources.
   *
   * The resourceActionGenerator returns an Iterable<ResourceAction> for each resource.
   *
   * If every resource-action in the iterable is authorized, the resource will be added to the filtered resources.
   *
   * If there is an authorization failure for one of the resource-actions, the resource will not be
   * added to the returned filtered resources..
   *
   * If the resourceActionGenerator returns null for a resource, that resource will not be added to the filtered
   * resources.
   *
   * @param authenticationResult    Authentication result representing identity of requester
   * @param resources               resources to be processed into resource-actions
   * @param resourceActionGenerator Function that creates an iterable of resource-actions from a resource
   * @param authorizerMapper        authorizer mapper
   *
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
    final Iterable<ResType> filteredResources = Iterables.filter(
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

    return filteredResources;
  }

  /**
   * Given a map of resource lists, filter each resources list by applying the resource action generator to each
   * item in each resource list.
   *
   * The resourceActionGenerator returns an Iterable<ResourceAction> for each resource.
   *
   * If a resource list is null or has no authorized items after filtering, it will not be included in the returned
   * map.
   *
   * This function will set the DRUID_AUTHORIZATION_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request                 HTTP request to be authorized
   * @param unfilteredResources     Map of resource lists to be filtered
   * @param resourceActionGenerator Function that creates an iterable of resource-actions from a resource
   * @param authorizerMapper        authorizer mapper
   *
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

      if (filteredList.size() > 0) {
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
   * This method constructs a 'superuser' set of permissions composed of {@link Action#READ} and {@link Action#WRITE}
   * permissions for all known {@link ResourceType#knownTypes()} for any {@link Authorizer} implementation which is
   * built on pattern matching with a regex.
   *
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

  /**
   * Function for the common pattern of generating a resource-action for reading from a view, using the
   * view name.
   */
  public static final Function<String, ResourceAction> VIEW_READ_RA_GENERATOR = input -> new ResourceAction(
      new Resource(input, ResourceType.VIEW),
      Action.READ
  );

  /**
   * Function for the pattern of generating a {@link ResourceAction} for reading from a given {@link Resource}
   */
  public static final Function<Resource, ResourceAction> RESOURCE_READ_RA_GENERATOR = input -> new ResourceAction(
      input,
      Action.READ
  );
}
