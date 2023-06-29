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

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * {@link JsonInclude} filter for {@link StringEncoding} that ignores UTF16LE, which is the typical default
 * for aggregators.
 *
 * This API works by "creative" use of equals. It requires warnings to be suppressed and also requires spotbugs
 * exclusions (see spotbugs-exclude.xml).
 */
@SuppressWarnings({"EqualsAndHashcode", "EqualsHashCode", "EqualsWhichDoesntCheckParameterClass"})
public class StringEncodingDefaultUTF16LEJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
{
  @Override
  public boolean equals(Object obj)
  {
    return obj == StringEncoding.UTF16LE;
  }
}
