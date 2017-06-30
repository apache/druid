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

package io.druid.server.http.security;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.java.util.common.StringUtils;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import org.easymock.EasyMock;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ResourceFilterTestHelper
{
  public HttpServletRequest req;
  public AuthorizationInfo authorizationInfo;
  public ContainerRequest request;

  public void setUp(ResourceFilter resourceFilter) throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    request = EasyMock.createStrictMock(ContainerRequest.class);
    authorizationInfo = EasyMock.createStrictMock(AuthorizationInfo.class);

    // Memory barrier
    synchronized (this) {
      ((AbstractResourceFilter) resourceFilter).setReq(req);
    }
  }

  public void setUpMockExpectations(
      String requestPath,
      boolean authCheckResult,
      String requestMethod
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
    EasyMock.expect(req.getAttribute(EasyMock.anyString())).andReturn(authorizationInfo).atLeastOnce();
    EasyMock.expect(authorizationInfo.isAuthorized(
        EasyMock.anyObject(Resource.class),
        EasyMock.anyObject(Action.class)
    )).andReturn(
        new Access(authCheckResult)
    ).atLeastOnce();

  }

  public static Collection<Object[]> getRequestPaths(final Class clazz)
  {
    return getRequestPaths(clazz, ImmutableList.<Class<?>>of(), ImmutableList.<Key<?>>of());
  }

  public static Collection<Object[]> getRequestPaths(
      final Class clazz,
      final Iterable<Class<?>> mockableInjections
  )
  {
    return getRequestPaths(clazz, mockableInjections, ImmutableList.<Key<?>>of());
  }

  public static Collection<Object[]> getRequestPaths(
      final Class clazz,
      final Iterable<Class<?>> mockableInjections,
      final Iterable<Key<?>> mockableKeys
  )
  {
    return getRequestPaths(clazz, mockableInjections, mockableKeys, ImmutableList.of());
  }

  // Feeds in an array of [ PathName, MethodName, ResourceFilter , Injector]
  public static Collection<Object[]> getRequestPaths(
      final Class clazz,
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
            binder.bind(AuthConfig.class).toInstance(new AuthConfig(true));
          }
        }
    );
    final String basepath = ((Path) clazz.getAnnotation(Path.class)).value().substring(1); //Ignore the first "/"
    final List<Class<? extends ResourceFilter>> baseResourceFilters =
        clazz.getAnnotation(ResourceFilters.class) == null ? Collections.<Class<? extends ResourceFilter>>emptyList() :
        ImmutableList.copyOf(((ResourceFilters) clazz.getAnnotation(ResourceFilters.class)).value());

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
                    ImmutableList.copyOf(clazz.getDeclaredMethods()),
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

                    return Collections2.transform(
                        resourceFilters,
                        new Function<Class<? extends ResourceFilter>, Object[]>()
                        {
                          @Override
                          public Object[] apply(Class<? extends ResourceFilter> input)
                          {
                            if (method.getAnnotation(Path.class) != null) {
                              return new Object[]{
                                  StringUtils.format("%s%s", basepath, method.getAnnotation(Path.class).value()),
                                  input.getAnnotation(GET.class) == null ? (method.getAnnotation(DELETE.class) == null
                                                                            ? "POST"
                                                                            : "DELETE") : "GET",
                                  injector.getInstance(input),
                                  injector
                              };
                            } else {
                              return new Object[]{
                                  basepath,
                                  input.getAnnotation(GET.class) == null ? (method.getAnnotation(DELETE.class) == null
                                                                            ? "POST"
                                                                            : "DELETE") : "GET",
                                  injector.getInstance(input),
                                  injector
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
}
