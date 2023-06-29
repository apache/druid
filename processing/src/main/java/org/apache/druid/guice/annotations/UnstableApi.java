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

package org.apache.druid.guice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signifies that the annotated entity is an unstable API for extension authors. Unstable APIs may change at any time
 * in breaking ways even between minor Druid release lines (e.g., 0.16.0 -> 0.16.1).
 *
 * All public and protected fields, methods, and constructors of annotated classes and interfaces are considered
 * unstable in this sense.
 *
 * Unstable APIs can become {@link PublicApi}s or {@link ExtensionPoint}s once they settle down. This change can happen
 * only between major Druid release lines (e.g., 0.16.0 -> 0.17.0).
 *
 * @see PublicApi
 * @see ExtensionPoint
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface UnstableApi
{
}
