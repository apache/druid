/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.config;

import org.joda.time.Duration;
import org.joda.time.Period;
import org.skife.config.Coercer;
import org.skife.config.Coercible;

/**
*/
public class DurationCoercible implements Coercible<Duration>
{
  @Override
  public Coercer<Duration> accept(Class<?> clazz)
  {
    if (Duration.class != clazz) {
      return null;
    }

    return new Coercer<Duration>()
    {
      @Override
      public Duration coerce(String value)
      {
        return new Period(value).toStandardDuration();
      }
    };
  }
}
