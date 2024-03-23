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

package org.apache.druid.sql.calcite;

import com.google.common.base.Throwables;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.opentest4j.IncompleteExecutionException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertThrows;

/**
 * Can be used to mark tests which are not-yet supported for some reason.
 *
 * In case a testcase marked with this annotation fails - it means that the
 * testcase no longer fails with the annotated expectation. This means that a
 * code change affected this test either
 *
 * <ol>
 * <li>it suddenly passes: yay, assuming it makes sense that it suddenly passes,
 * remove the annotation and move on</li>
 * <li>it suddenly fails with a different error: validate that the new error is
 * expected and either fix to continue failing with the old error or update the
 * expected error.</li>
 * </ol>
 *
 * During usage; the annotation process have to be added to registered with the testclass.
 * Ensure that it's loaded as the most outer-rule by using the right ExtendWith order - or by
 * specifying Order:
 * <code>
 *   @Order(0)
 *   @RegisterExtension
 *   public TestRule notYetSupportedRule = new NotYetSupportedProcessor();
 *
 *   @NotYetSupported(NOT_ENOUGH_RULES)
 *   &#64;Test
 *   public void testA() {
 *   }
 * </code>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface NotYetSupported
{
  Modes value();

  enum Modes
  {
    // @formatter:off
    NOT_ENOUGH_RULES(DruidException.class, "not enough rules"),
    ERROR_HANDLING(AssertionError.class, "targetPersona: is <[A-Z]+> and category: is <[A-Z_]+> and errorCode: is"),
    EXPRESSION_NOT_GROUPED(DruidException.class, "Expression '[a-z]+' is not being grouped"),
    COLUMN_NOT_FOUND(DruidException.class, "CalciteContextException.*Column.*not found in any table"),
    NULLS_FIRST_LAST(DruidException.class, "NULLS (FIRST|LAST)"),
    BIGINT_TO_DATE(DruidException.class, "BIGINT to type (DATE|TIME)"),
    NPE(DruidException.class, "java.lang.NullPointerException"),
    AGGREGATION_NOT_SUPPORT_TYPE(DruidException.class, "Aggregation \\[(MIN|MAX)\\] does not support type \\[STRING\\]"),
    RESULT_COUNT_MISMATCH(AssertionError.class, "result count:"),
    ALLDATA_CSV(DruidException.class, "allData.csv"),
    BIGINT_TIME_COMPARE(DruidException.class, "Cannot apply '.' to arguments of type"),
    INCORRECT_SYNTAX(DruidException.class, "Incorrect syntax near the keyword"),
    RESULT_MISMATCH(AssertionError.class, "(assertResulEquals|AssertionError: column content mismatch)"),
    UNSUPPORTED_NULL_ORDERING(DruidException.class, "(A|DE)SCENDING ordering with NULLS (LAST|FIRST)"),
    WINDOW_OPERATOR_QUERY_ON_UNSUPPORTED_DATASOURCE(DruidException.class, "WindowOperatorQuery must run on top of a query or inline data source"),
    UNION_WITH_COMPLEX_OPERAND(DruidException.class, "Only Table and Values are supported as inputs for Union"),
    UNION_MORE_STRICT_ROWTYPE_CHECK(DruidException.class, "Row signature mismatch in Union inputs"),
    JOIN_CONDITION_NOT_PUSHED_CONDITION(DruidException.class, "SQL requires a join with '.*' condition"),
    JOIN_CONDITION_UNSUPORTED_OPERAND(DruidException.class, "SQL .* unsupported operand type"),
    JOIN_TABLE_TABLE(ISE.class, "Cannot handle subquery structure for dataSource: JoinDataSource"),
    CORRELATE_CONVERSION(DruidException.class, "Missing conversion( is|s are) LogicalCorrelate"),
    SORT_REMOVE_TROUBLE(DruidException.class, "Calcite assertion violated.*Sort\\.<init>"),
    STACK_OVERFLOW(StackOverflowError.class, ""),
    CANNOT_JOIN_LOOKUP_NON_KEY(RuntimeException.class, "Cannot join lookup with condition referring to non-key");
    // @formatter:on

    public Class<? extends Throwable> throwableClass;
    public String regex;

    Modes(Class<? extends Throwable> cl, String regex)
    {
      this.throwableClass = cl;
      this.regex = regex;
    }

    Pattern getPattern()
    {
      return Pattern.compile(regex, Pattern.MULTILINE | Pattern.DOTALL);
    }
  };

  /**
   * Processes {@link NotYetSupported} annotations.
   *
   * Ensures that test cases disabled with that annotation can still not pass.
   * If the error is as expected; the testcase is marked as "ignored".
   */
  class NotYetSupportedProcessor implements InvocationInterceptor
  {
    @Override
    public void interceptTestMethod(Invocation<Void> invocation,
        ReflectiveInvocationContext<Method> invocationContext,
        ExtensionContext extensionContext) throws Throwable
    {
      Method method = extensionContext.getTestMethod().get();
      NotYetSupported annotation = method.getAnnotation(NotYetSupported.class);

      if (annotation == null) {
        invocation.proceed();
        return;
      }
      {
        {
          Modes ignoreMode = annotation.value();
          Throwable e = null;
          try {
            invocation.proceed();
          }
          catch (Throwable t) {
            e = t;
          }
          // If the base test case is supposed to be ignored already, just skip
          // the further evaluation
          if (e instanceof AssumptionViolatedException) {
            throw (AssumptionViolatedException) e;
          }
          if (e instanceof IncompleteExecutionException) {
            throw (IncompleteExecutionException) e;
          }
          Throwable finalE = e;
          assertThrows(
              "Expected that this testcase will fail - it might got fixed; or failure have changed?",
              ignoreMode.throwableClass,
              () -> {
                if (finalE != null) {
                  throw finalE;
                }
              }
          );

          String trace = Throwables.getStackTraceAsString(e);
          Matcher m = annotation.value().getPattern().matcher(trace);

          if (!m.find()) {
            throw new AssertionError("Exception stactrace doesn't match regex: " + annotation.value().regex, e);
          }
          throw new AssumptionViolatedException("Test is not-yet supported; ignored with:" + annotation);
        }
      }
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation,
        ReflectiveInvocationContext<Method> invocationContext,
        ExtensionContext extensionContext) throws Throwable
    {
      interceptTestMethod(invocation, invocationContext, extensionContext);
    }
  }
}
