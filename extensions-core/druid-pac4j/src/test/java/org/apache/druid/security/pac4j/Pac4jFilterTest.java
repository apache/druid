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

package org.apache.druid.security.pac4j;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.exception.http.ForbiddenAction;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.core.exception.http.WithLocationAction;
import org.pac4j.core.http.adapter.JEEHttpActionAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class Pac4jFilterTest
{

  @Mock
  private HttpServletRequest request;
  @Mock
  private HttpServletResponse response;
  private JEEContext context;

  @Before
  public void setUp()
  {
    context = new JEEContext(request, response);
  }

  @Test
  public void testActionAdapterForRedirection()
  {
    HttpAction httpAction = new FoundAction("testUrl");
    Mockito.doReturn(httpAction.getCode()).when(response).getStatus();
    Mockito.doReturn(((WithLocationAction) httpAction).getLocation()).when(response).getHeader(any());
    JEEHttpActionAdapter.INSTANCE.adapt(httpAction, context);
    Assert.assertEquals(response.getStatus(), 302);
    Assert.assertEquals(response.getHeader("Location"), "testUrl");
  }

  @Test
  public void testActionAdapterForForbidden()
  {
    HttpAction httpAction = ForbiddenAction.INSTANCE;
    Mockito.doReturn(httpAction.getCode()).when(response).getStatus();
    JEEHttpActionAdapter.INSTANCE.adapt(httpAction, context);
    Assert.assertEquals(response.getStatus(), HttpServletResponse.SC_FORBIDDEN);
  }

}
