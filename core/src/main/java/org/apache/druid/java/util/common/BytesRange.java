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

package org.apache.druid.java.util.common;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({
    ElementType.FIELD,
    ElementType.PARAMETER
})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = BytesRange.DateRangeValidator.class)
public @interface BytesRange
{

  long min() default 0;

  long max() default Long.MAX_VALUE;

  //ConstraintValidator requires
  Class<?>[] groups() default {};

  //ConstraintValidator requires
  String message() default "value is out of range";

  //ConstraintValidator requires
  Class<? extends Payload>[] payload() default {};

  class DateRangeValidator implements ConstraintValidator<BytesRange, Object>
  {
    private BytesRange range;

    @Override
    public void initialize(BytesRange range)
    {
      this.range = range;
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context)
    {
      if (value == null) {
        return true;
      }
      if (value instanceof Bytes) {
        long lvalue = ((Bytes) value).getValue();
        return lvalue >= range.min() &&
               lvalue <= range.max();
      }
      return true;
    }
  }
}
