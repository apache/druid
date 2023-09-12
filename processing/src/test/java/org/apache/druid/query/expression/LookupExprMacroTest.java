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

package org.apache.druid.query.expression;

import com.google.common.collect.Lists;
import org.apache.commons.compress.utils.Sets;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LookupExprMacroTest extends MacroTestBase
{
  public LookupExprMacroTest()
  {
    super(
        new LookupExprMacro(new LookupExtractorFactoryContainerProvider()
        {
          @Override
          public Set<String> getAllLookupNames()
          {
            return Sets.newHashSet("test_lookup");
          }

          @Override
          public Optional<LookupExtractorFactoryContainer> get(String lookupName)
          {
            return Optional.empty();
          }
        })
    );
  }

  @Test
  public void testTooFewArgs()
  {
    expectException(IllegalArgumentException.class, "Function[lookup] requires 2 to 3 arguments");
    apply(Collections.emptyList());
  }

  @Test
  public void testNonLiteralLookupName()
  {
    expectException(
        IllegalArgumentException.class,
        "Function[lookup] second argument must be a registered lookup name"
    );
    apply(getArgs(Lists.newArrayList("1", new ArrayList<String>())));
  }

  @Test
  public void testValidCalls()
  {
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("1", "test_lookup"))));
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("null", "test_lookup"))));
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("1", "test_lookup", null))));
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("1", "test_lookup", "N/A"))));
  }

  private List<Expr> getArgs(List<Object> args)
  {
    return args.stream().map(a -> {
      if (a != null && a instanceof String) {
        return ExprEval.of(a.toString()).toExpr();
      }
      return ExprEval.bestEffortOf(null).toExpr();
    }).collect(Collectors.toList());
  }
}
