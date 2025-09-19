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

package org.apache.druid.msq.test;

import org.apache.druid.sql.calcite.CalciteNestedDataQueryTest;
import org.apache.druid.sql.calcite.NotYetSupported;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.extension.RegisterExtension;

@SqlTestFrameworkConfig.ComponentSupplier(DecoupledDartCalciteNestedDataQueryTest.NestedDataQueryMSQComponentSupplier.class)
public class DecoupledDartCalciteNestedDataQueryTest extends CalciteNestedDataQueryTest
{

  public static class NestedDataQueryMSQComponentSupplier extends AbstractDartComponentSupplier
  {
    public NestedDataQueryMSQComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(new NestedComponentSupplier(tempFolderProducer));
    }
  }

  @RegisterExtension
  NotYetSupportedProcessor notYetSupportedProcessor = new NotYetSupportedProcessor(NotYetSupported.Scope.DECOUPLED_DART);

  @RegisterExtension
  DecoupledDartExtension decoupledExtension = new DecoupledDartExtension(this);

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return decoupledExtension.testBuilder();
  }

  @Override
  protected void msqIncompatible()
  {
    throw new AssumptionViolatedException("Case marked as msqIncompatible; not trying dart right now");
  }
}
