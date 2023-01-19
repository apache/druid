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

package org.apache.druid.guice;

import com.google.inject.ScopeAnnotation;
import org.apache.druid.guice.annotations.PublicApi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A replacement for Guice's Singleton scope.
 *
 * This scope exists because deep down in the bowels of Guice, there are times when it looks for bindings of Singleton
 * scope and magically promotes them to be eagerly bound.  When Guice eagerly instantiates things, it does it in an
 * order that is not based on the object dependency graph, which can do things like cause objects to be registered on
 * the lifecycle out-of-dependency order.
 *
 * Generally speaking, LazySingleton should <em>always</em> be used and {@link com.google.inject.Singleton} should
 * <em>never</em> be used.  That said, there might be times when interacting with external libraries that make it
 * necessary to use Singleton.  In these cases, it is best to figure out if there is a way to use LazySingleton, if it
 * is prohibitively difficult to use LazySingleton, then Singleton should be used with knowledge that the object will
 * be instantiated in a non-deterministic ordering compared to other objects in the JVM.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ScopeAnnotation
@PublicApi
public @interface LazySingleton
{
}
