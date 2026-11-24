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

import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class QueryHandlerTest
{
  private final Interpreter interpreter = Mockito.mock(Interpreter.class);

  @Test
  public void testEnumerateClosesTheEnumeratorAndTheInterpreterOnceTheResultsAreConsumed()
  {
    final Enumerator<Object[]> enumerator = Mockito.spy(Linq4j.enumerator(List.<Object[]>of(new Object[]{1L})));
    Mockito.when(interpreter.enumerator()).thenReturn(enumerator);

    Assertions.assertEquals(1, QueryHandler.enumerate(interpreter).toList().size());

    Mockito.verify(enumerator).close();
    Mockito.verify(interpreter).close();
  }

  @Test
  public void testEnumerateClosesTheInterpreterIfEnumerationFails()
  {
    Mockito.when(interpreter.enumerator()).thenThrow(new ISE("Interpreter node failed"));

    Assertions.assertThrows(ISE.class, () -> QueryHandler.enumerate(interpreter).toList());

    Mockito.verify(interpreter).close();
  }
}
