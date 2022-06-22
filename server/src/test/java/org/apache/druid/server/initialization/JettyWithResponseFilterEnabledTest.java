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

package org.apache.druid.server.initialization;

import org.junit.Assert;
import org.junit.Test;

public class JettyWithResponseFilterEnabledTest extends JettyTest
{
  @Override
  public void setProperties()
  {
    // call super.setProperties first in case it is setting the same property as this class
    super.setProperties();
    System.setProperty("druid.server.http.showDetailedJettyErrors", "false");
  }

  @Test
  @Override
  public void testJettyErrorHandlerWithFilter()
  {
    // Response filter is enabled by config hence we do not show servlet information
    Assert.assertFalse(server.getErrorHandler().isShowServlet());
  }
}
