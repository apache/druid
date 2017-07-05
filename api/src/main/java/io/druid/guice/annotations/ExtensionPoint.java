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

package io.druid.guice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signifies that the annotated type is an extension point. Extension points are interfaces or non-final classes that
 * may be subclassed in extensions in order to add functionality to Druid. Extension points may change in breaking ways
 * only between major Druid release lines (e.g. 0.10.x -> 0.11.0), but otherwise must remain stable. Extension points
 * may change at any time in non-breaking ways, however, such as by adding new default methods to an interface.
 *
 * All public and protected fields, methods, and constructors of annotated classes and interfaces are considered
 * stable in this sense. If a class is not annotated, but an individual field, method, or constructor is
 * annotated, then only that particular field, method, or constructor is considered an extension API.
 *
 * Extension points are all considered public APIs in the sense of {@link PublicApi}, even if not explicitly annotated
 * as such.
 *
 * Note that there are number of injectable interfaces that are not annotated with {@code ExtensionPoint}. You may
 * still extend these interfaces in extensions, but your extension may need to be recompiled even for a minor
 * update of Druid.
 *
 * @see PublicApi
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface ExtensionPoint
{
}
