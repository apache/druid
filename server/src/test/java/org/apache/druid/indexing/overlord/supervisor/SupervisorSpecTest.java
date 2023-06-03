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

package org.apache.druid.indexing.overlord.supervisor;

import org.apache.druid.java.util.common.UOE;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SupervisorSpecTest
{
  private static final SupervisorSpec SUPERVISOR_SPEC = new SupervisorSpec()
  {
    @Override
    public String getId()
    {
      return null;
    }

    @Override
    public Supervisor createSupervisor()
    {
      return null;
    }

    @Override
    public List<String> getDataSources()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public String getSource()
    {
      return null;
    }
  };

  @Test
  public void test()
  {
    Assert.assertThrows(UOE.class, () -> SUPERVISOR_SPEC.getInputSourceResources());
  }
}
