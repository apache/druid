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

package org.apache.druid.indexing.overlord.http.security;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.spi.container.ContainerRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.PathSegment;
import java.util.function.Consumer;

@RunWith(MockitoJUnitRunner.class)
public class AuthorizerResourceFilterTest
{
  private static final String VALID_NAME = "VALID_NAME";
  private static final String INVALID_NAME = "INVALID_NAME";

  @Mock
  private ContainerRequest request;
  @Mock
  private PathSegment authorizerNamePathSegmentMarker;
  @Mock
  private PathSegment authorizerNamePathSegment;
  @Mock
  private Consumer<String> authorizerNameValidator;

  private AuthorizerResourceFilter target;

  @Before
  public void setUp()
  {
    Mockito.doThrow(IllegalArgumentException.class).when(authorizerNameValidator).accept(INVALID_NAME);
    Mockito.when(authorizerNamePathSegmentMarker.getPath()).thenReturn("authorizerName");
    Mockito.when(authorizerNamePathSegment.getPath()).thenReturn(VALID_NAME);
    Mockito.when(request.getPathSegments()).thenReturn(
        ImmutableList.of(authorizerNamePathSegmentMarker, authorizerNamePathSegment)
    );
    target = new AuthorizerResourceFilter(authorizerNameValidator);
  }
  @Test
  public void testGetRequestFilterShouldReturnSelf()
  {
    Assert.assertSame(target, target.getRequestFilter());
  }

  @Test
  public void testGetResponseFilterShouldReturnNull()
  {
    Assert.assertNull(target.getResponseFilter());
  }

  @Test
  public void testFilterWithValidNameShouldDoNothing()
  {
    target.filter(request);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFilterWithInvalidNameShouldThrowException()
  {
    Mockito.doReturn(INVALID_NAME).when(authorizerNamePathSegment).getPath();
    target.filter(request);
  }
}
