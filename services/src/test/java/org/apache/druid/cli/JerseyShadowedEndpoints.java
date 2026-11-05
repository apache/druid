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

package org.apache.druid.cli;

import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.api.model.AbstractResource;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.api.model.AbstractSubResourceLocator;
import com.sun.jersey.api.model.AbstractSubResourceMethod;
import com.sun.jersey.server.impl.modelapi.annotation.IntrospectionModeller;
import com.sun.jersey.server.impl.uri.PathPattern;
import com.sun.jersey.server.impl.uri.PathTemplate;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Finds Jersey endpoints that no request can reach, because the root resource class of another endpoint shadows them.
 * <p>
 * Jersey 1.x routes a request in two steps. Out of the root resource classes whose {@code @Path} matches the request
 * path, it first picks the one that sorts first under {@link PathPattern#COMPARATOR}, which prefers paths with more
 * literal characters. It then looks for a matching method in that class only. It never falls back to another class,
 * so if the chosen class has no matching method, the request fails with a 404 even when another class declares that
 * exact endpoint. For example, an endpoint at {@code /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks} in a
 * class with {@code @Path("/druid/indexer/v1")} stops working once another class has
 * {@code @Path("/druid/indexer/v1/datasources")}.
 * <p>
 * To apply exactly the same rules, this class uses Jersey's own resource model and path matching.
 */
final class JerseyShadowedEndpoints
{
  /**
   * Substituted for every path parameter to build a request path that reaches an endpoint.
   */
  private static final String PATH_PARAMETER_VALUE = "x";

  private JerseyShadowedEndpoints()
  {
  }

  /**
   * Returns a description of every shadowed endpoint of the given resource classes, or an empty list if every endpoint
   * is reachable. Classes without {@code @Path}, such as providers, are ignored.
   */
  static List<String> find(final Collection<Class<?>> resourceClasses)
  {
    // In the order in which Jersey tries root resources.
    final List<RootResource> rootResources =
        resourceClasses.stream()
                       .map(IntrospectionModeller::createResource)
                       .filter(AbstractResource::isRootResource)
                       .map(RootResource::new)
                       .sorted((r1, r2) -> PathPattern.COMPARATOR.compare(r1.pattern, r2.pattern))
                       .collect(Collectors.toList());

    final List<String> shadowedEndpoints = new ArrayList<>();
    for (final RootResource rootResource : rootResources) {
      for (final Endpoint endpoint : rootResource.getEndpoints()) {
        if (!rootResource.matches(endpoint.requestPath)) {
          throw new ISE("Request path[%s] built for %s does not match its own resource class", endpoint.requestPath, endpoint);
        }
        final RootResource routedTo = findRoutedResource(rootResources, endpoint.requestPath);
        if (routedTo != rootResource) {
          shadowedEndpoints.add(StringUtils.format("%s is shadowed by %s", endpoint, routedTo));
        }
      }
    }
    return shadowedEndpoints;
  }

  private static RootResource findRoutedResource(final List<RootResource> sortedRootResources, final String requestPath)
  {
    for (final RootResource rootResource : sortedRootResources) {
      if (rootResource.matches(requestPath)) {
        return rootResource;
      }
    }
    throw new ISE("No resource class matches request path[%s]", requestPath);
  }

  private static class RootResource
  {
    private final AbstractResource resource;
    private final PathPattern pattern;

    private RootResource(final AbstractResource resource)
    {
      this.resource = resource;
      // Built the same way as in Jersey's RootResourceUriRules.
      this.pattern = new PathPattern(new PathTemplate(resource.getPath().getValue()));
    }

    private boolean matches(final String requestPath)
    {
      return pattern.match(requestPath) != null;
    }

    private List<Endpoint> getEndpoints()
    {
      final String path = resource.getPath().getValue();
      final List<Endpoint> endpoints = new ArrayList<>();
      for (final AbstractResourceMethod method : resource.getResourceMethods()) {
        endpoints.add(new Endpoint(method.getHttpMethod(), path, method));
      }
      for (final AbstractSubResourceMethod method : resource.getSubResourceMethods()) {
        endpoints.add(new Endpoint(method.getHttpMethod(), concatPaths(path, method.getPath().getValue()), method));
      }
      for (final AbstractSubResourceLocator locator : resource.getSubResourceLocators()) {
        endpoints.add(new Endpoint("ANY", concatPaths(path, locator.getPath().getValue()), locator));
      }
      return endpoints;
    }

    private static String concatPaths(final String path, final String subPath)
    {
      return StringUtils.maybeRemoveTrailingSlash(path) + "/" + StringUtils.maybeRemoveLeadingSlash(subPath);
    }

    @Override
    public String toString()
    {
      return StringUtils.format(
          "[%s] with @Path[%s]",
          resource.getResourceClass().getName(),
          resource.getPath().getValue()
      );
    }
  }

  private static class Endpoint
  {
    private final String httpMethod;
    private final String path;
    private final AbstractMethod method;
    private final String requestPath;

    private Endpoint(final String httpMethod, final String path, final AbstractMethod method)
    {
      this.httpMethod = httpMethod;
      this.path = path;
      this.method = method;

      final PathTemplate template = new PathTemplate(path);
      final Map<String, String> pathParameters = new HashMap<>();
      for (final String name : template.getTemplateVariables()) {
        pathParameters.put(name, PATH_PARAMETER_VALUE);
      }
      this.requestPath = template.createURI(pathParameters);
    }

    @Override
    public String toString()
    {
      return StringUtils.format(
          "[%s %s] of [%s.%s()]",
          httpMethod,
          path,
          method.getMethod().getDeclaringClass().getName(),
          method.getMethod().getName()
      );
    }
  }
}
