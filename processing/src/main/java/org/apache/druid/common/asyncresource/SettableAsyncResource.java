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

package org.apache.druid.common.asyncresource;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Basic utility that allows creating {@link AsyncResource} wrappers. Analogous to JDK {@link CompletableFuture}
 * or Guava {@link SettableFuture}. See {@link AsyncResource} for details about why you would want to use this
 * instead of {@link Future}.
 *
 * <p>In addition to {@link #get()}, there is also {@link #release()}. Releasing is not allowed by instances
 * of this class, but may be allowed by subclasses.
 */
public class SettableAsyncResource<T> implements AsyncResource<T>
{
  private static final Logger LOG = new Logger(SettableAsyncResource.class);

  /**
   * Whether {@link #release()} is allowed.
   */
  private final boolean releasable;

  /**
   * Callbacks provided by {@link #addReadyCallback(Runnable)}.
   */
  @GuardedBy("this")
  private final List<Runnable> readyCallbacks = new ArrayList<>();

  /**
   * Canceler provided by {@link #setCanceler(Runnable)}. Ideally would be {@code GuardeBy("this")}, but isn't
   * annotated because errorprone doesn't like the reference to canceler::run escaping.
   */
  @Nullable
  private Runnable canceler;

  /**
   * Result set by {@link #setInternal(Either)}.
   */
  @Nullable
  @GuardedBy("this")
  private ResourceHolder<T> result = null;

  /**
   * Error set by {@link #setInternal(Either)}.
   */
  @Nullable
  @GuardedBy("this")
  private Throwable error = null;

  @GuardedBy("this")
  private State state = State.NEW;

  /**
   * Constructor.
   */
  public SettableAsyncResource()
  {
    this(false);
  }

  /**
   * Constructor for subclasses that allow {@link #release()}. They should provide "true" here and then
   * also override {@link #release()} to change its visibility from protected to public.
   */
  protected SettableAsyncResource(boolean releasable)
  {
    this.releasable = releasable;
  }

  /**
   * Set a canceler that will be called from {@link #close()} if this {@link AsyncResource} is closed prior
   * to the resource being available. Calling this method has no effect if {@link #close()} has already been called
   * or if the resource is already available.
   */
  public synchronized void setCanceler(Runnable newCanceler)
  {
    if (canceler != null) {
      throw DruidException.defensive("canceler already set, cannot call setCanceler()");
    }

    if (state == State.NEW) {
      this.canceler = newCanceler;
    }
  }

  /**
   * Provides a resource and closer for the resource. Transitions the {@link AsyncResource} into a "ready" state,
   * where {@link #isReady()} returns true, if it has not yet been closed. Returns true if this transition happened
   * successfully, false otherwise.
   *
   * <p>If this method returns true, it also fires all the callbacks that were registered via
   * {@link #addReadyCallback(Runnable)}. Once this method returns true, {@link #close()} will no longer call
   * the canceler from {@link #setCanceler(Runnable)}.
   *
   * <p>If this method returns false, the producer is responsible for closing the resource itself.
   *
   * <p>Throws {@link DruidException} if this resource was already completed from a prior call to this method or
   * {@link #setException}).
   */
  public boolean set(T object, @Nullable Closeable closer)
  {
    if (object == null) {
      throw DruidException.defensive("object cannot be null");
    }
    final ResourceHolder<T> resourceHolder = new ResourceHolder<>()
    {
      @Override
      public T get()
      {
        return object;
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
    return setInternal(Either.value(resourceHolder));
  }

  /**
   * Variant of {@link #set(Object, Closeable)} for callers that have a {@link ResourceHolder}.
   */
  public boolean set(ResourceHolder<T> holder)
  {
    return set(holder.get(), holder);
  }

  /**
   * Provides an exception for a resource that failed to load. Transitions the {@link AsyncResource} into a "ready"
   * state, where {@link #isReady()} returns true, if it has not yet been closed.
   *
   * <p>If this method successfully transitions to "ready", it also fires all the callbacks that were registered via
   * {@link #addReadyCallback(Runnable)}. Afterwards, {@link #close()} will no longer call the canceler from
   * {@link #setCanceler(Runnable)}.
   *
   * <p>Throws {@link DruidException} if this resource was already completed from a prior call to this method or
   * {@link #set}).
   */
  public void setException(Throwable t)
  {
    setInternal(Either.error(t));
  }

  @Override
  public synchronized boolean isReady()
  {
    return state == State.READY;
  }

  @Override
  public synchronized T get()
  {
    return switch (state) {
      case NEW -> throw DruidException.defensive("Not ready yet");
      case READY -> {
        if (error != null) {
          Throwables.throwIfUnchecked(error);
          throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                              .build(error, error.getMessage());
        } else {
          yield result.get();
        }
      }
      case RELEASED -> throw DruidException.defensive("Resource has been released");
      case CLOSED -> throw DruidException.defensive("Closed");
    };
  }

  /**
   * Take ownership of the underlying object. After this returns, {@link #close()} on this
   * {@link AsyncResource} is a no-op; the caller is responsible for closing the returned {@code T}. Useful when
   * passing the resource to something else that prefers to take full ownership of it.
   *
   * <p>Throws {@link DruidException} if the holder is not yet ready, has already been released, or if
   * {@link #close()} has been called.
   */
  protected synchronized T release()
  {
    if (!releasable) {
      throw DruidException.defensive("Not releasable");
    }

    final T object = get();
    // Clear result to allow GC.
    result = null;
    state = State.RELEASED;
    return object;
  }

  @Override
  public void addReadyCallback(Runnable callback)
  {
    final boolean fireImmediately;
    synchronized (this) {
      switch (state) {
        case NEW -> {
          readyCallbacks.add(callback);
          fireImmediately = false;
        }
        case READY -> fireImmediately = true;
        default -> throw DruidException.defensive("Cannot addReadyCallback in state[%s]", state);
      }
    }
    if (fireImmediately) {
      callback.run();
    }
  }

  @Override
  public void close()
  {
    final Closeable deferredCloseable;

    synchronized (this) {
      deferredCloseable = switch (state) {
        case NEW -> canceler != null ? canceler::run : null;
        case READY -> result;
        case RELEASED -> null;
        default -> throw DruidException.defensive("Already closed");
      };

      // Clear result and canceler to allow GC.
      result = null;
      canceler = null;
      state = State.CLOSED;
    }

    CloseableUtils.closeAndSuppressExceptions(
        deferredCloseable,
        e -> LOG.warn(e, "Failed to call cleaner of class[%s]", deferredCloseable.getClass())
    );
  }

  @GuardedBy("this")
  private List<Runnable> drainCallbacks()
  {
    final List<Runnable> snapshot = List.copyOf(readyCallbacks);
    readyCallbacks.clear();
    return snapshot;
  }

  private boolean setInternal(Either<Throwable, ResourceHolder<T>> value)
  {
    final boolean didSet;
    final List<Runnable> callbacksToFire;

    synchronized (this) {
      didSet = switch (state) {
        case NEW -> {
          if (value.isError()) {
            error = value.error();
          } else {
            result = value.valueOrThrow();
          }

          state = State.READY;
          yield true;
        }
        case READY, RELEASED -> throw DruidException.defensive("Already complete, cannot call set/setException again");
        case CLOSED -> false;
      };

      // Clear canceler to allow GC.
      canceler = null;
      callbacksToFire = drainCallbacks();
    }
    fireCallbacks(callbacksToFire);
    return didSet;
  }

  private static void fireCallbacks(List<Runnable> callbacks)
  {
    for (final Runnable callback : callbacks) {
      try {
        callback.run();
      }
      catch (Throwable t) {
        // Best-effort; one bad callback shouldn't break others.
        LOG.warn(t, "callback exception");
      }
    }
  }

  enum State
  {
    NEW,
    READY,
    RELEASED,
    CLOSED
  }
}
