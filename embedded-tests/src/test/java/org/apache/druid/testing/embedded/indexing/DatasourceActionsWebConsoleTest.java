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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.testing.embedded.console.WebConsoleTestBase;
import org.junit.jupiter.api.Test;

/**
 * Marks the segments of a datasource unused and used again, then deletes them with a kill task, from the Datasources
 * view of the web console.
 */
public class DatasourceActionsWebConsoleTest extends WebConsoleTestBase
{
  @Test
  public void testDatasourceActions() throws Exception
  {
    runSpec("datasource-actions.spec.ts");
  }
}
