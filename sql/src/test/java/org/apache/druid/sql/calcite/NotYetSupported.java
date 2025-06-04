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
 * code change affected this test either.
 *
 * Each reason must belong to a {@link Scope}; for which the
 * {@link NotYetSupportedProcessor} be enable to be supressed.
 *
 * <ol>
 * <li>it suddenly passes: yay, assuming it makes sense that it suddenly passes,
 * remove the annotation and move on</li>
 * <li>it suddenly fails with a different error: validate that the new error is
 * expected and either fix to continue failing with the old error or update the
 * expected error.</li>
 * </ol>
 *
 * During usage; the annotation process have to be added to registered with the
 * testclass. Ensure that it's loaded as the most outer-rule by using the right
 * ExtendWith order - or by specifying Order: <code>
 *   &#64;Order(0)
 *   @RegisterExtension
 *   public TestRule notYetSupportedRule = new NotYetSupportedProcessor(Scope.DECOUPLED);
 *
 *   &#64;NotYetSupported(NOT_ENOUGH_RULES)
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

  enum Scope
  {
    WINDOWING,
    DECOUPLED,
    BINDABLE,
  }

  enum Modes
  {
    // @formatter:off
    DISTINCT_AGGREGATE_NOT_SUPPORTED(Scope.WINDOWING, DruidException.class, "DISTINCT is not supported"),
    NULLS_FIRST_LAST(Scope.WINDOWING, DruidException.class, "NULLS (FIRST|LAST)"),
    BIGINT_TO_DATE(Scope.WINDOWING, DruidException.class, "BIGINT to type (DATE|TIME)"),
    AGGREGATION_NOT_SUPPORT_TYPE(Scope.WINDOWING, DruidException.class, "Aggregation \\[(MIN|MAX)\\] does not support type \\[STRING\\]"),
    ALLDATA_CSV(Scope.WINDOWING, DruidException.class, "allData.csv"),
    BIGINT_TIME_COMPARE(Scope.WINDOWING, DruidException.class, "Cannot apply '.' to arguments of type"),
    VIEWS_NOT_SUPPORTED(Scope.WINDOWING, DruidException.class, "Incorrect syntax near the keyword 'CREATE'"),
    RESULT_MISMATCH(Scope.WINDOWING, AssertionError.class, "(assertResulEquals|AssertionError: column content mismatch)"),
    LONG_CASTING(Scope.WINDOWING, AssertionError.class, "expected: java.lang.Long"),
    UNSUPPORTED_NULL_ORDERING(Scope.WINDOWING, DruidException.class, "(A|DE)SCENDING ordering with NULLS (LAST|FIRST)"),

    EXPRESSION_NOT_GROUPED(Scope.BINDABLE, DruidException.class, "Expression '[a-z]+' is not being grouped"),

    NOT_ENOUGH_RULES(Scope.DECOUPLED, DruidException.class, "There are not enough rules to produce a node"),
    SORT_REMOVE_TROUBLE(Scope.DECOUPLED, DruidException.class, "Calcite assertion violated.*Sort\\.<init>"),
    SORT_REMOVE_CONSTANT_KEYS_CONFLICT(Scope.DECOUPLED, DruidException.class, "not enough rules"),
    UNNEST_INLINED(Scope.DECOUPLED, Exception.class, "Missing conversion is Uncollect"),
    UNNEST_RESULT_MISMATCH(Scope.DECOUPLED, AssertionError.class, "(Result count mismatch|column content mismatch)"),
    SUPPORT_SORT(Scope.DECOUPLED, DruidException.class, "Unable to process relNode.*DruidSort"),
    SUPPORT_AGGREGATE(Scope.DECOUPLED, DruidException.class, "Unable to process relNode.*DruidAggregate"),
    RESTRICTED_DATASOURCE_SUPPORT(Scope.DECOUPLED, DruidException.class, "ForbiddenException: Unauthorized");
    // @formatter:on

    public Scope scope;
    public Class<? extends Throwable> throwableClass;
    public String regex;

    Modes(Scope scope, Class<? extends Throwable> cl, String regex)
    {
      this.scope = scope;
      this.throwableClass = cl;
      this.regex = regex;
    }

    Modes(Class<? extends Throwable> cl, String regex)
    {
      this(Scope.DECOUPLED, cl, regex);
    }

    Pattern getPattern()
    {
      return Pattern.compile(regex, Pattern.MULTILINE | Pattern.DOTALL);
    }

    @Override
    public String toString()
    {
      return name() + "{" + regex + "}";
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
    private final Scope scope;

    public NotYetSupportedProcessor(Scope scope)
    {
      this.scope = scope;
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation,
        ReflectiveInvocationContext<Method> invocationContext,
        ExtensionContext extensionContext) throws Throwable
    {
      Method method = extensionContext.getTestMethod().get();
      NotYetSupported annotation = method.getAnnotation(NotYetSupported.class);

      if (annotation == null || annotation.value().scope != scope) {
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
            throw new AssertionError("Exception stacktrace doesn't match regex: " + annotation.value().regex, e);
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
