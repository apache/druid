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

package org.apache.druid.msq.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReaderProvider;
import org.apache.druid.msq.input.InputSpecSlicerProvider;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.query.Query;

import java.lang.annotation.Annotation;

/**
 * Utility class for MSQ-related Guice bindings.
 */
public class MSQBinders
{
  /**
   * Creates a MapBinder for QueryKit implementations. Extensions can use this
   * to register their own QueryKit implementations for custom query types.
   *
   * Example usage:
   * <pre>
   * MSQBinders.queryKitBinder(binder)
   *     .addBinding(MyCustomQuery.class)
   *     .to(MyCustomQueryKit.class);
   * </pre>
   */
  @SuppressWarnings("rawtypes")
  public static MapBinder<Class<? extends Query>, QueryKit> queryKitBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        new TypeLiteral<>() {}
    );
  }

  /**
   * Bind an {@link InputSpecSlicerProvider} for use on a controller. The annotation should be
   * {@link IndexingService} for providers used by tasks, or {@link Dart} for providers used by Dart.
   */
  public static Multibinder<InputSpecSlicerProvider> inputSpecSlicerProviderBinder(
      Binder binder,
      Class<? extends Annotation> annotation
  )
  {
    return Multibinder.newSetBinder(binder, Key.get(InputSpecSlicerProvider.class, annotation));
  }

  /**
   * Bind an {@link InputSliceReaderProvider} for use on a worker, to handle a particular {@link InputSlice}.
   * The annotation should be {@link IndexingService} for providers used by tasks, or {@link Dart} for providers
   * used by Dart.
   */
  public static Multibinder<InputSliceReaderProvider> inputSliceReaderProviderBinder(
      Binder binder,
      Class<? extends Annotation> annotation
  )
  {
    return Multibinder.newSetBinder(binder, Key.get(InputSliceReaderProvider.class, annotation));
  }
}
