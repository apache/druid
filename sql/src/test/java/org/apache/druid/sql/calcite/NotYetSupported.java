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
import org.apache.druid.java.util.common.UOE;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertThrows;

/**
 * Can be used to mark tests which are not-yet supported for some reason.
 *
 * In case a testcase marked with this annotation fails - it means that the
 * testcase no longer fails with the annotated expectation. This means that a code change affected this test either
 *
 * <ol>
 * <li>it suddenly passes: yay, assuming it makes sense that it suddenly passes, remove the annotation and move on</li>
 * <li>it suddenly fails with a different error: validate that the new error is expected and either fix to continue failing with the old error or update the expected error.</li>
 * </ol>
 *
 * During usage; the annotation process have to be added to the testclass.
 * Ensure that it's loaded as the most outer-rule by using order=0 - otherwise
 * it may interfere with other rules:
 * <code>
 *   @Rule(order = 0)
 *   public TestRule notYetSupportedRule = new NotYetSupportedProcessor();
 *
 *   @NotYetSupported(NOT_ENOUGH_RULES)
 *   @Test
 *   public void testA() {
 *   }
 * </code>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface NotYetSupported
{
  Modes value() default Modes.NOT_ENOUGH_RULES;

  enum Modes
  {
    PLAN_MISMATCH(AssertionError.class, "AssertionError: query #"),
    NOT_ENOUGH_RULES(DruidException.class, "not enough rules"),
    CANNOT_CONVERT(DruidException.class, "Cannot convert query parts"),
    ERROR_HANDLING(AssertionError.class, "(is <ADMIN> was <OPERATOR>|is <INVALID_INPUT> was <UNCATEGORIZED>|with message a string containing)"),
    EXPRESSION_NOT_GROUPED(DruidException.class, "Expression '[a-z]+' is not being grouped"),
    COLUMN_NOT_FOUND(DruidException.class, "CalciteContextException.*Column.*not found in any table"),
    NULLS_FIRST_LAST(DruidException.class, "NULLS (FIRST|LAST)"),
    BIGINT_TO_DATE(DruidException.class, "BIGINT to type (DATE|TIME)"),
    NPE_PLAIN(NullPointerException.class, "java.lang.NullPointerException"),
    NPE(DruidException.class, "java.lang.NullPointerException"),
    AGGREGATION_NOT_SUPPORT_TYPE(DruidException.class, "Aggregation \\[(MIN|MAX)\\] does not support type \\[STRING\\]"),
    CANNOT_APPLY_VIRTUAL_COL(UOE.class, "apply virtual columns"),
    MISSING_DESC(DruidException.class, "function signature DESC"),
    RESULT_COUNT_MISMATCH(AssertionError.class, "result count:"),
    ALLDATA_CSV(DruidException.class, "allData.csv"),
    BIGINT_TIME_COMPARE(DruidException.class, "Cannot apply '.' to arguments of type"),
    INCORRECT_SYNTAX(DruidException.class, "Incorrect syntax near the keyword"),
    // at least c7 is represented oddly in the parquet file
    T_ALLTYPES_ISSUES(AssertionError.class, "(t_alltype|allTypsUniq|fewRowsAllData).parquet.*Verifier.verify"),
    RESULT_MISMATCH(AssertionError.class, "assertResultsEquals"),
    UNSUPPORTED_NULL_ORDERING(DruidException.class, "(A|DE)SCENDING ordering with NULLS (LAST|FIRST)"),
    CANNOT_TRANSLATE(DruidException.class, "Cannot translate reference");

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
  class NotYetSupportedProcessor implements TestRule
  {
    @Override
    public Statement apply(Statement base, Description description)
    {
      NotYetSupported annotation = description.getAnnotation(NotYetSupported.class);

      if (annotation == null) {
        return base;
      }
      return new Statement()
      {
        @Override
        public void evaluate()
        {
          Modes ignoreMode = annotation.value();
          Throwable e = assertThrows(
              "Expected that this testcase will fail - it might got fixed; or failure have changed?",
              ignoreMode.throwableClass,
              base::evaluate
              );

          String trace = Throwables.getStackTraceAsString(e);
          Matcher m = annotation.value().getPattern().matcher(trace);

          if (!m.find()) {
            throw new AssertionError("Exception stactrace doesn't match regex: " + annotation.value().regex, e);
          }
          throw new AssumptionViolatedException("Test is not-yet supported; ignored with:" + annotation);
        }
      };
    }
  }
}
