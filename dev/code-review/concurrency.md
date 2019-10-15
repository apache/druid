<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

## Druid's Checklist for Concurrency Code

Design
 - [Concurrency is rationalized in the PR description?](
 https://github.com/code-review-checklists/java-concurrency#rationalize)
 - [Can use patterns to simplify concurrency?](https://github.com/code-review-checklists/java-concurrency#use-patterns)
   - Immutability/Snapshotting
   - Divide and conquer
   - Producer-consumer
   - Instance confinement
   - Thread/Task/Serial thread confinement
   - Active object

Documentation
 - [Thread safety is justified in comments?](
 https://github.com/code-review-checklists/java-concurrency#justify-document)
 - [Class (method, field) has concurrent access documentation?](
 https://github.com/code-review-checklists/java-concurrency#justify-document)
 - [Threading model of a subsystem (class) is described?](
 https://github.com/code-review-checklists/java-concurrency#threading-flow-model)
 - [Concurrent control flow (or data flow) of a subsystem (class) is described?](
 https://github.com/code-review-checklists/java-concurrency#threading-flow-model)
 - [Class is documented as immutable, thread-safe, or not thread-safe?](
 https://github.com/code-review-checklists/java-concurrency#immutable-thread-safe)
 - [Used concurrency patterns are pronounced?](
 https://github.com/code-review-checklists/java-concurrency#name-patterns)
 - [`@GuardedBy` annotation is used?](https://github.com/code-review-checklists/java-concurrency#guarded-by)
 - [Safety of a benign race (e. g. unbalanced synchronization) is explained?](
 https://github.com/code-review-checklists/java-concurrency#document-benign-race)
 - [Each use of `volatile` is justified?](https://github.com/code-review-checklists/java-concurrency#justify-volatile)
 - [Each field that is neither `volatile` nor annotated with `@GuardedBy` has a comment?](
 https://github.com/code-review-checklists/java-concurrency#plain-field)

Insufficient synchronization
 - [Static methods and fields are thread-safe?](
 https://github.com/code-review-checklists/java-concurrency#static-thread-safe)
 - [Thread *doesn't* wait in a loop for a non-volatile field to be updated by another thread?](
 https://github.com/code-review-checklists/java-concurrency#non-volatile-visibility)
 - [Read access to a non-volatile, concurrently updatable primitive field is protected?](
 https://github.com/code-review-checklists/java-concurrency#non-volatile-protection)
 - [`@GET`/`@POST` methods, Jetty Filters and Handlers are thread-safe?](
 https://github.com/code-review-checklists/java-concurrency#server-framework-sync)
 - [Calls to `DateFormat.parse()` and `format()` are synchronized?](
 https://github.com/code-review-checklists/java-concurrency#dateformat)

Excessive thread safety
 - [No "extra" (pseudo) thread safety?](https://github.com/code-review-checklists/java-concurrency#pseudo-safety)
 - [No atomics on which only `get()` and `set()` are called?](
 https://github.com/code-review-checklists/java-concurrency#redundant-atomics)
 - [Class (method) needs to be thread-safe?](
 https://github.com/code-review-checklists/java-concurrency#unneeded-thread-safety)
 - [`ReentrantLock` (`ReentrantReadWriteLock`, `Semaphore`) needs to be fair?](
 https://github.com/code-review-checklists/java-concurrency#unneeded-fairness)

Race conditions
 - [No `put()` or `remove()` calls on a `ConcurrentMap` (or Cache) after `get()` or `containsKey()`?](
 https://github.com/code-review-checklists/java-concurrency#chm-race)
 - [No point accesses to a non-thread-safe collection outside of critical sections?](
 https://github.com/code-review-checklists/java-concurrency#unsafe-concurrent-point-read)
 - [Iteration over a non-thread-safe collection doesn't leak outside of a critical section?](
 https://github.com/code-review-checklists/java-concurrency#unsafe-concurrent-iteration)
 - [Non-trivial object is *not* returned from a getter in a thread-safe class?](
 https://github.com/code-review-checklists/java-concurrency#concurrent-mutation-race)
 - [No separate getters to an atomically updated state?](
 https://github.com/code-review-checklists/java-concurrency#moving-state-race)
 - [No *check-then-act* race conditions (state used inside a critical section is read outside of it)?](
 https://github.com/code-review-checklists/java-concurrency#read-outside-critical-section-race)
 - [`coll.toArray(new E[coll.size()])` is *not* called on a synchronized collection?](
 https://github.com/code-review-checklists/java-concurrency#read-outside-critical-section-race)
 - [No race conditions with user or programmatic input or interop between programs?](
 https://github.com/code-review-checklists/java-concurrency#outside-world-race)
 - [No check-then-act race conditions with file system operations?](
 https://github.com/code-review-checklists/java-concurrency#outside-world-race)
 - [No concurrent `invalidate(key)` and `get()` calls  on Guava's loading `Cache`?](
 https://github.com/code-review-checklists/java-concurrency#guava-cache-invalidation-race)
 - [`Cache.put()` is not used (nor exposed in the own Cache interface)?](
 https://github.com/code-review-checklists/java-concurrency#cache-invalidation-race)
 - [Concurrent invalidation race is not possible on a lazily initialized state?](
 https://github.com/code-review-checklists/java-concurrency#cache-invalidation-race)
 - [Iteration, Stream pipeline, or copying a `Collections.synchronized*()` collection is protected by a lock?](
 https://github.com/code-review-checklists/java-concurrency#synchronized-collection-iter)

Testing
 - [Unit tests for thread-safe classes are multi-threaded?](
 https://github.com/code-review-checklists/java-concurrency#multi-threaded-tests)
 - [A shared `Random` instance is *not* used from concurrent test workers?](
 https://github.com/code-review-checklists/java-concurrency#concurrent-test-random)
 - [Concurrent test workers coordinate their start?](
 https://github.com/code-review-checklists/java-concurrency#coordinate-test-workers)
 - [There are more test threads than CPUs (if possible for the test)?](
 https://github.com/code-review-checklists/java-concurrency#test-workers-interleavings)

Locks
 - [Can use `LifecycleLock` instead of a standard lock in a lifecycled object?](#use-lifecycle-lock)
 - [Can use some concurrency utility instead of a lock with conditional `wait` (`await`) calls?](
 https://github.com/code-review-checklists/java-concurrency#avoid-wait-notify)
 - [Can use Guava’s `Monitor` instead of a lock with conditional `wait` (`await`) calls?](
 https://github.com/code-review-checklists/java-concurrency#guava-monitor)
 - [Can use `synchronized` instead of a `ReentrantLock`?](
 https://github.com/code-review-checklists/java-concurrency#use-synchronized)
 - [`lock()` is called outside of `try {}`? No statements between `lock()` and `try {}`?](
 https://github.com/code-review-checklists/java-concurrency#lock-unlock)

Avoiding deadlocks
 - [Can avoid nested critical sections?](
 https://github.com/code-review-checklists/java-concurrency#avoid-nested-critical-sections)
 - [Locking order for nested critical sections is documented?](
 https://github.com/code-review-checklists/java-concurrency#document-locking-order)
 - [Dynamically determined locks for nested critical sections are ordered?](
 https://github.com/code-review-checklists/java-concurrency#dynamic-lock-ordering)
 - [No extension API calls within critical sections?](
 https://github.com/code-review-checklists/java-concurrency#non-open-call)
 - [No calls to `ConcurrentHashMap`'s methods (incl. `get()`) in `compute()`-like lambdas on the same map?](
 https://github.com/code-review-checklists/java-concurrency#chm-nested-calls)

Improving scalability
 - [Critical section is as small as possible?](
 https://github.com/code-review-checklists/java-concurrency#minimize-critical-sections)
 - [Can use `ConcurrentHashMap.compute()` or Guava's `Striped` for per-key locking?](
 https://github.com/code-review-checklists/java-concurrency#increase-locking-granularity)
 - [Can replace blocking collection or a queue with a concurrent one?](
 https://github.com/code-review-checklists/java-concurrency#non-blocking-collections)
 - [Can use `ClassValue` instead of `ConcurrentHashMap<Class, ...>`?](
 https://github.com/code-review-checklists/java-concurrency#use-class-value)
 - [Considered `ReadWriteLock` (or `StampedLock`) instead of a simple lock?](
 https://github.com/code-review-checklists/java-concurrency#read-write-lock)
 - [`StampedLock` is used instead of `ReadWriteLock` when reentrancy is not needed?](
 https://github.com/code-review-checklists/java-concurrency#use-stamped-lock)
 - [Considered `LongAdder` instead of an `AtomicLong` for a "hot field"?](
 https://github.com/code-review-checklists/java-concurrency#long-adder-for-hot-fields)
 - [Considered queues from JCTools instead of the standard concurrent queues?](
 https://github.com/code-review-checklists/java-concurrency#jctools)
 - [Caffeine cache is used instead of Guava?](https://github.com/apache/incubator-druid/issues/8399)
 - [Can apply speculation (optimistic concurrency) technique?](
 https://github.com/code-review-checklists/java-concurrency#speculation)

Lazy initialization and double-checked locking
 - [Lazy initialization of a field should be thread-safe?](
 https://github.com/code-review-checklists/java-concurrency#lazy-init-thread-safety)
 - [Considered double-checked locking for a lazy initialization to improve performance?](
 https://github.com/code-review-checklists/java-concurrency#use-dcl)
 - [Double-checked locking follows the SafeLocalDCL pattern?](
 https://github.com/code-review-checklists/java-concurrency#safe-local-dcl)
  - [Considered eager initialization instead of a lazy initialization to simplify code?](
  https://github.com/code-review-checklists/java-concurrency#eager-init)
 - [Can do lazy initialization with a benign race and without locking to improve performance?](
 https://github.com/code-review-checklists/java-concurrency#lazy-init-benign-race)
 - [Holder class idiom is used for lazy static fields rather than double-checked locking?](
 https://github.com/code-review-checklists/java-concurrency#no-static-dcl)

Non-blocking and partially blocking code
 - [Non-blocking code has enough comments to make line-by-line checking as easy as possible?](
 https://github.com/code-review-checklists/java-concurrency#check-non-blocking-code)
 - [Can use immutable POJO + compare-and-swap operations to simplify non-blocking code?](
 https://github.com/code-review-checklists/java-concurrency#swap-state-atomically)
 - [Boundaries of non-blocking or benignly racy code are identified with WARNING comments?](
 https://github.com/code-review-checklists/java-concurrency#non-blocking-warning)

Threads and Executors
 - [Thread is named?](https://github.com/code-review-checklists/java-concurrency#name-threads)
 - [Thread is daemon?](#daemon-threads)
 - [Using `Execs` to create an `ExecutorService`?](#use-execs)
 - [Can use `ExecutorService` instead of creating a new `Thread` each time some method is called?](
 https://github.com/code-review-checklists/java-concurrency#reuse-threads)
 - [No network I/O in a CachedThreadPool?](
 https://github.com/code-review-checklists/java-concurrency#cached-thread-pool-no-io)
 - [No blocking (incl. I/O) operations in a `ForkJoinPool` or in a parallel Stream pipeline?](
 https://github.com/code-review-checklists/java-concurrency#fjp-no-blocking)
 - [Can execute non-blocking computation in `FJP.commonPool()` instead of a custom thread pool?](
 https://github.com/code-review-checklists/java-concurrency#use-common-fjp)
 - [`ExecutorService` is shutdown explicitly?](
 https://github.com/code-review-checklists/java-concurrency#explicit-shutdown)

Parallel Streams
 - [Parallel Stream computation takes more than 100us in total?](
 https://github.com/code-review-checklists/java-concurrency#justify-parallel-stream-use)
 - [Comment before a parallel Streams pipeline explains how it takes more than 100us in total?](
 https://github.com/code-review-checklists/java-concurrency#justify-parallel-stream-use)
 
Thread interruption and `Future` cancellation
 - [Interruption status is restored before wrapping `InterruptedException` with another exception?](
 https://github.com/code-review-checklists/java-concurrency#restore-interruption)
 - [`InterruptedException` is swallowed only in the following kinds of methods](
 https://github.com/code-review-checklists/java-concurrency#interruption-swallowing):
   - `Runnable.run()`, `Callable.call()`, or methods to be passed to executors as lambda tasks; or
   - Methods with "try" or "best effort" semantics?
 - [`InterruptedException` swallowing is documented for a method?](
 https://github.com/code-review-checklists/java-concurrency#interruption-swallowing)
 - [Can use Guava's `Uninterruptibles` to avoid `InterruptedException` swallowing?](
 https://github.com/code-review-checklists/java-concurrency#interruption-swallowing)
 - [`Future` is canceled upon catching an `InterruptedException` or a `TimeoutException` on `get()`?](
 https://github.com/code-review-checklists/java-concurrency#cancel-future)

Time
 - [`nanoTime()` values are compared in an overflow-aware manner?](
 https://github.com/code-review-checklists/java-concurrency#nano-time-overflow)
 - [`currentTimeMillis()` is *not* used to measure time intervals and timeouts?](
 https://github.com/code-review-checklists/java-concurrency#time-going-backward)
 - [Units for a time variable are identified in the variable's name or via `TimeUnit`?](
 https://github.com/code-review-checklists/java-concurrency#time-units)
 - [Negative timeouts and delays are treated as zeros?](
 https://github.com/code-review-checklists/java-concurrency#treat-negative-timeout-as-zero)

Thread safety of Cleaners and native code
 - [`close()` is concurrently idempotent in a class with a `Cleaner` or `finalize()`?](
 https://github.com/code-review-checklists/java-concurrency#thread-safe-close-with-cleaner)
 - [Method accessing native state calls `reachabilityFence()` in a class with a `Cleaner` or `finalize()`?](
 https://github.com/code-review-checklists/java-concurrency#reachability-fence)
 - [`Cleaner` or `finalize()` is used for real cleanup, not mere reporting?](
 https://github.com/code-review-checklists/java-concurrency#finalize-misuse)
 - [Considered making a class with native state thread-safe?](
 https://github.com/code-review-checklists/java-concurrency#thread-safe-native)
 
<hr>

<a name="use-lifecycle-lock"></a>
[#](#use-lifecycle-lock) Lk.D1. Is it possible to use Druid's `LifecycleLock` utility instead of a standard lock and
"started" flag in lifecycled objects with `start()` and `stop()` methods? See the Javadoc comment for `LifecycleLock`
for more details.

<a name="daemon-threads"></a>
[#](#daemon-threads) TE.D1. Are Threads created directly or via a `ThreadFactory` configured to be daemon via
`setDaemon(true)`? Note that by default, ThreadFactories constructed via `Execs.makeThreadFactory()` methods create
daemon threads already.

<a name="use-execs"></a>
[#](#use-execs) TE.D2. Is it possible to use one of the static factory methods in Druid's `Execs` utility class to
create an `ExecutorService` instead of Java's standard `ExecutorServices`? This is recommended because `Execs` configure
ThreadFactories to create daemon threads by default, as required by the previous item.