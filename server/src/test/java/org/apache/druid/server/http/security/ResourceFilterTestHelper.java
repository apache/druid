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

package org.apache.druid.server.http.security;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.easymock.EasyMock;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ResourceFilterTestHelper
{
  private static final Set<Class<? extends ResourceFilter>> LEGACY_FILTERS = ImmutableSet.of(
      ConfigResourceFilter.class,
      StateResourceFilter.class
  );

  public static Set<Class<? extends ResourceFilter>> currentFilters = Sets.newHashSet(
      DatasourceResourceFilter.class,
      RulesResourceFilter.class
  );

  private static final Set<Class<? extends ResourceFilter>> NEW_FILTERS = ImmutableSet.of(
      InternalInternalResourceFilter.class,
      ServerServerResourceFilter.class,
      ServerStatusResourceFilter.class,
      ServerUserResourceFilter.class
  );

  public HttpServletRequest req;
  public AuthorizerMapper authorizerMapper;
  public ContainerRequest request;

  public void setUp(ResourceFilter resourceFilter)
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    request = EasyMock.createStrictMock(ContainerRequest.class);
    authorizerMapper = EasyMock.createMock(AuthorizerMapper.class);

    // Memory barrier
    synchronized (this) {
      ((AbstractResourceFilter) resourceFilter).setReq(req);
      ((AbstractResourceFilter) resourceFilter).setAuthorizerMapper(authorizerMapper);
    }
  }

  public void setUpMockExpectations(
      String requestPath,
      boolean authCheckResult,
      String requestMethod,
      String authVersion,
      boolean performAuth
  )
  {
    EasyMock.expect(request.getPath()).andReturn(requestPath).anyTimes();
    EasyMock.expect(request.getPathSegments()).andReturn(
        ImmutableList.copyOf(
            Iterables.transform(
                Arrays.asList(requestPath.split("/")),
                new Function<String, PathSegment>()
                {
                  @Override
                  public PathSegment apply(final String input)
                  {
                    return new PathSegment()
                    {
                      @Override
                      public String getPath()
                      {
                        return input;
                      }

                      @Override
                      public MultivaluedMap<String, String> getMatrixParameters()
                      {
                        return null;
                      }
                    };
                  }
                }
            )
        )
    ).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn(requestMethod).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).anyTimes();
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);

    EasyMock.expect(authorizerMapper.getAuthVersion()).andReturn(authVersion).anyTimes();
    if (performAuth) {
      EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
          .andReturn(authenticationResult)
          .atLeastOnce();
      EasyMock.expect(authorizerMapper.getAuthorizer(
          EasyMock.anyString()
      )).andReturn(
          new Authorizer()
          {
            @Override
            public Access authorize(AuthenticationResult authenticationResult1, Resource resource, Action action)
            {
              return new Access(authCheckResult);
            }
            @Override
            public Access authorizeV2(AuthenticationResult authenticationResult1, Resource resource, Action action)
            {
              return new Access(authCheckResult);
            }
          }
      ).atLeastOnce();
      req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, authCheckResult);
      EasyMock.expectLastCall().anyTimes();
    }
  }

  public static Collection<Object[]> getRequestPathsWithAuthorizer(final AnnotatedElement classOrMethod)
  {
    return getRequestPaths(classOrMethod, ImmutableList.of(AuthorizerMapper.class), ImmutableList.of());
  }

  public static Collection<Object[]> getRequestPaths(
      final Class clazz,
      final Iterable<Class<?>> mockableInjections
  )
  {
    return getRequestPaths(clazz, mockableInjections, ImmutableList.of());
  }

  public static Collection<Object[]> getRequestPaths(
      final AnnotatedElement classOrMethod,
      final Iterable<Class<?>> mockableInjections,
      final Iterable<Key<?>> mockableKeys
  )
  {
    return getRequestPaths(classOrMethod, mockableInjections, mockableKeys, ImmutableList.of());
  }

  // Feeds in an array of [ PathName, MethodName, ResourceFilter , Injector]
  public static Collection<Object[]> getRequestPaths(
      final AnnotatedElement classOrMethod,
      final Iterable<Class<?>> mockableInjections,
      final Iterable<Key<?>> mockableKeys,
      final Iterable<?> injectedObjs
  )
  {
    final Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            for (Class clazz : mockableInjections) {
              binder.bind(clazz).toInstance(EasyMock.createNiceMock(clazz));
            }
            for (Object obj : injectedObjs) {
              binder.bind((Class) obj.getClass()).toInstance(obj);
            }
            for (Key<?> key : mockableKeys) {
              binder.bind((Key<Object>) key).toInstance(EasyMock.createNiceMock(key.getTypeLiteral().getRawType()));
            }
            binder.bind(AuthConfig.class).toInstance(new AuthConfig());
          }
        }
    );
    final String basepath = classOrMethod.getAnnotation(Path.class).value().substring(1); //Ignore the first "/"
    final List<Class<? extends ResourceFilter>> baseResourceFilters =
        classOrMethod.getAnnotation(ResourceFilters.class) == null ? Collections.emptyList() :
        ImmutableList.copyOf(classOrMethod.getAnnotation(ResourceFilters.class).value());

    List<Method> methods;
    if (classOrMethod instanceof Class<?>) {
      methods = ImmutableList.copyOf(((Class<?>) classOrMethod).getDeclaredMethods());
    } else {
      methods = Collections.singletonList((Method) classOrMethod);
    }
    return ImmutableList.copyOf(
        Iterables.concat(
            // Step 3 - Merge all the Objects arrays for each endpoints
            Iterables.transform(
                // Step 2 -
                // For each endpoint, make an Object array containing
                //  - Request Path like "druid/../../.."
                //  - Request Method like "GET" or "POST" or "DELETE"
                //  - Resource Filter instance for the endpoint
                Iterables.filter(
                    // Step 1 -
                    // Filter out non resource endpoint methods
                    // and also the endpoints that does not have any
                    // ResourceFilters applied to them
                    methods,
                    new Predicate<Method>()
                    {
                      @Override
                      public boolean apply(Method input)
                      {
                        return input.getAnnotation(GET.class) != null
                               || input.getAnnotation(POST.class) != null
                               || input.getAnnotation(DELETE.class) != null
                                  && (input.getAnnotation(ResourceFilters.class) != null
                                      || !baseResourceFilters.isEmpty());
                      }
                    }
                ),
                new Function<Method, Collection<Object[]>>()
                {
                  @Override
                  public Collection<Object[]> apply(final Method method)
                  {
                    final List<Class<? extends ResourceFilter>> resourceFilters =
                        method.getAnnotation(ResourceFilters.class) == null ? baseResourceFilters :
                            ImmutableList.copyOf(method.getAnnotation(ResourceFilters.class).value());
                    final List<Pair<Class<? extends ResourceFilter>, String>> filterVersionPairs = resourceFilters
                        .stream().flatMap(
                            (java.util.function.Function<Class<? extends ResourceFilter>, Stream<Pair<Class<? extends ResourceFilter>, String>>>) filter ->
                                Stream.of(
                                    new Pair<>(filter, AuthConfig.AUTH_VERSION_1),
                                    new Pair<>(filter, AuthConfig.AUTH_VERSION_2)
                                )).collect(Collectors.toList());
                    return Collections2.transform(
                        filterVersionPairs,
                        new Function<Pair<Class<? extends ResourceFilter>, String>, Object[]>()
                        {
                          @Override
                          public Object[] apply(Pair<Class<? extends ResourceFilter>, String> filterVersionPair)
                          {
                            if (method.getAnnotation(Path.class) != null) {
                              return new Object[] {
                                  StringUtils.format("%s%s", basepath, method.getAnnotation(Path.class).value()),
                                  httpMethodFromAnnotation(filterVersionPair.lhs, method),
                                  injector.getInstance(filterVersionPair.lhs),
                                  injector,
                                  filterVersionPair.rhs,
                                  performAuth(filterVersionPair.lhs, filterVersionPair.rhs)
                              };
                            } else {
                              return new Object[] {
                                  basepath,
                                  httpMethodFromAnnotation(filterVersionPair.lhs, method),
                                  injector.getInstance(filterVersionPair.lhs),
                                  injector,
                                  filterVersionPair.rhs,
                                  performAuth(filterVersionPair.lhs, filterVersionPair.rhs)
                              };
                            }
                          }
                        }
                    );
                  }
                }
            )
        )
    );
  }

  private static String httpMethodFromAnnotation(Class<? extends ResourceFilter> input, Method method)
  {
    if (input.getAnnotation(GET.class) != null) {
      return "GET";
    } else {
      return method.getAnnotation(DELETE.class) != null ? "DELETE" : "POST";
    }
  }

  // the new filters will perform authorization only if auth version is v2
  // current filters mostly datasource realted ones will work same for both v1 and v2
  // legacy filters will perform authorization only if auth version is v1
  public static boolean performAuth(Class<? extends ResourceFilter> filter, String authVersion)
  {
    if (NEW_FILTERS.contains(filter) && authVersion.equals(AuthConfig.AUTH_VERSION_2)) {
      return true;
    } else if (currentFilters.contains(filter)) {
      return true;
    } else {
      return LEGACY_FILTERS.contains(filter) && authVersion.equals(AuthConfig.AUTH_VERSION_1);
    }
  }
}
