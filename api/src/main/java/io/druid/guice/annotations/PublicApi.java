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
 * Signifies that the annotated entity is a public API for extension authors. Public APIs may change in breaking ways
 * only between major Druid release lines (e.g. 0.10.x -> 0.11.0), but otherwise must remain stable. Public APIs may
 * change at any time in non-breaking ways, however, such as by adding new fields, methods, or constructors.
 *
 * Note that interfaces annotated with {@code PublicApi} but not with {@link ExtensionPoint} are not meant to be
 * subclassed in extensions. In this case, the annotation simply signifies that the interface is stable for callers.
 * In particular, since it is not meant to be subclassed, new non-default methods may be added to an interface and
 * new abstract methods may be added to a class.
 *
 * If a class or interface is annotated, then all public and protected fields, methods, and constructors that class
 * or interface are considered stable in this sense. If a class is not annotated, but an individual field, method, or
 * constructor is annotated, then only that particular field, method, or constructor is considered a public API.
 *
 * Classes, fields, method, and constructors _not_ annotated with {@code @PublicApi} may be modified or removed
 * in any Druid release, unless they are annotated with {@link ExtensionPoint} (which implies they are a public API
 * as well).
 *
 * @see ExtensionPoint
 */
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.CONSTRUCTOR})
@Retention(RetentionPolicy.SOURCE)
public @interface PublicApi
{
}
