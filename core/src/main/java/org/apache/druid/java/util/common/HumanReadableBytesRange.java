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

/**
 * This annotation is an extension of java validation framework to ensure the validity of value of {@link HumanReadableBytes}.
 *
 * To use it, put it on a field of type of {@link HumanReadableBytes}.
 * For example,
 *
 * <code>
 *   class Size {
 *     @HumanReadableBytesRange( min = 5, max = 1024 )
 *     private HumanReadableBytes size;
 *   }
 * </code>
 *
 *
 */
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE, ElementType.CONSTRUCTOR, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = HumanReadableBytesRange.HumanReadableBytesRangeValidator.class)
public @interface HumanReadableBytesRange
{
  /**
   * lower bound of {@link HumanReadableBytes}. Inclusive
   */
  long min() default 0;

  /**
   * upper bound of {@link HumanReadableBytes}. Inclusive
   */
  long max() default Long.MAX_VALUE;

  //ConstraintValidator requires
  Class<?>[] groups() default {};

  //ConstraintValidator requires
  String message() default "value is out of range";

  //ConstraintValidator requires
  Class<? extends Payload>[] payload() default {};

  class HumanReadableBytesRangeValidator implements ConstraintValidator<HumanReadableBytesRange, Object>
  {
    private HumanReadableBytesRange range;

    @Override
    public void initialize(HumanReadableBytesRange range)
    {
      this.range = range;
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context)
    {
      if (value == null) {
        return true;
      }
      if (value instanceof HumanReadableBytes) {
        long bytes = ((HumanReadableBytes) value).getBytes();
        return bytes >= range.min() &&
               bytes <= range.max();
      }
      return true;
    }
  }
}
