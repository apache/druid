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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.external.HttpOperatorConversion;
import org.apache.druid.sql.calcite.external.InlineOperatorConversion;
import org.apache.druid.sql.calcite.external.LocalOperatorConversion;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class DruidOperatorTableTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Mock (answer = Answers.RETURNS_DEEP_STUBS) TableDefnRegistry tableDefnRegistry;

  @Mock TableFunction tableFunction;
  private DruidOperatorTable operatorTable;

  @Test
  public void test_operatorTable_with_operatorConversionDenyList_deniedConversionsUnavailable()
  {
    Mockito.when(tableDefnRegistry.jsonMapper()).thenReturn(OBJECT_MAPPER);
    Mockito.when(tableDefnRegistry.inputSourceDefnFor(any()).adHocTableFn()).thenReturn(tableFunction);
    final ExternalOperatorConversion externalOperatorConversion =
        new ExternalOperatorConversion(OBJECT_MAPPER);
    final HttpOperatorConversion httpOperatorConversion =
        new HttpOperatorConversion(tableDefnRegistry);
    final InlineOperatorConversion inlineOperatorConversion =
        new InlineOperatorConversion(tableDefnRegistry);
    final LocalOperatorConversion localOperatorConversion =
        new LocalOperatorConversion(tableDefnRegistry);
    operatorTable = new DruidOperatorTable(
        ImmutableSet.of(),
        ImmutableSet.of(
            externalOperatorConversion,
            httpOperatorConversion,
            inlineOperatorConversion,
            localOperatorConversion
        ),
        PlannerOperatorConfig.newInstance(ImmutableList.of("extern", "http", "inline", "localfiles"))
    );

    SqlOperatorConversion operatorConversion =
        operatorTable.lookupOperatorConversion(externalOperatorConversion.calciteOperator());
    Assert.assertNull(operatorConversion);

    operatorConversion = operatorTable.lookupOperatorConversion(httpOperatorConversion.calciteOperator());
    Assert.assertNull(operatorConversion);

    operatorConversion = operatorTable.lookupOperatorConversion(inlineOperatorConversion.calciteOperator());
    Assert.assertNull(operatorConversion);

    operatorConversion = operatorTable.lookupOperatorConversion(localOperatorConversion.calciteOperator());
    Assert.assertNull(operatorConversion);
  }

  @Test
  public void test_operatorTable_with_emptyOperatorConversionDenyList_conversionsAavailable()
  {
    Mockito.when(tableDefnRegistry.jsonMapper()).thenReturn(OBJECT_MAPPER);
    Mockito.when(tableDefnRegistry.inputSourceDefnFor(any()).adHocTableFn()).thenReturn(tableFunction);
    final ExternalOperatorConversion externalOperatorConversion =
        new ExternalOperatorConversion(OBJECT_MAPPER);
    final HttpOperatorConversion httpOperatorConversion =
        new HttpOperatorConversion(tableDefnRegistry);
    final InlineOperatorConversion inlineOperatorConversion =
        new InlineOperatorConversion(tableDefnRegistry);
    final LocalOperatorConversion localOperatorConversion =
        new LocalOperatorConversion(tableDefnRegistry);
    operatorTable = new DruidOperatorTable(
        ImmutableSet.of(),
        ImmutableSet.of(
            externalOperatorConversion,
            httpOperatorConversion,
            inlineOperatorConversion,
            localOperatorConversion
        ),
        new PlannerOperatorConfig()
    );

    SqlOperatorConversion operatorConversion =
        operatorTable.lookupOperatorConversion(externalOperatorConversion.calciteOperator());
    Assert.assertEquals(externalOperatorConversion, operatorConversion);

    operatorConversion = operatorTable.lookupOperatorConversion(httpOperatorConversion.calciteOperator());
    Assert.assertEquals(httpOperatorConversion, operatorConversion);

    operatorConversion = operatorTable.lookupOperatorConversion(inlineOperatorConversion.calciteOperator());
    Assert.assertEquals(inlineOperatorConversion, operatorConversion);

    operatorConversion = operatorTable.lookupOperatorConversion(localOperatorConversion.calciteOperator());
    Assert.assertEquals(localOperatorConversion, operatorConversion);
  }
}
