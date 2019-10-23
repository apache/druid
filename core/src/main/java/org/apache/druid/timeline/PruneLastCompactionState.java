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

package org.apache.druid.timeline;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annnotation is used to inject a boolean parameter into a {@link DataSegment} constructor, which prescribes to
 * drop deserialized "lastCompactionState" and don't store it in a field of a {@link DataSegment}.
 * "lastCompactionState" is used only on the coordinator, peons, and indexers.
 *
 * - In auto compaction of the coordinator, "lastCompactionState" is used to determine whether the given
 *   segment needs further compaction or not.
 * - In parallel indexing, "lastCompactionState" should be serialized and deserialized properly when
 *   the sub tasks report the pushed segments to the supervisor task.
 */
@Target({ElementType.PARAMETER, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface PruneLastCompactionState
{
}
