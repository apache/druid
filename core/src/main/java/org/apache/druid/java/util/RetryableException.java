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

package org.apache.druid.java.util;

import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.RetryUtils;

/**
 * This Exception class can be use with {@link RetryUtils}.
 * The method {@link RetryUtils#retry(RetryUtils.Task, Predicate, int)} retry condition (Predicate argument)
 * requires an exception to be thrown and applying the predicate to the thrown exception.
 * For cases where the task method does not throw an exception but still needs retrying,
 * the method can throw this RetryableException so that the RetryUtils can then retry the task
 */
public class RetryableException extends Exception
{
  public RetryableException(Throwable t)
  {
    super(t);
  }

}
