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

package io.druid.query.monomorphicprocessing;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.SimpleRemapper;
import sun.misc.Unsafe;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages class specialization during query processing.
 * Usage:
 *
 * String runtimeShape = stringRuntimeShape.of(bufferAggregator);
 * SpecializationState<ProcessingAlgorithm> specializationState = SpecializationService.getSpecializationState(
 *   ProcessingAlgorithmImpl.class,
 *   runtimeShape
 * );
 * ProcessingAlgorithm algorithm = specializationState.getSpecializedOrDefault(new ProcessingAlgorithmImpl());
 * long loopIterations = new ProcessingAlgorithmImpl().run(bufferAggregator, ...);
 * specializationState.accountLoopIterations(loopIterations);
 *
 * ProcessingAlgorithmImpl.class, passed as prototypeClass to {@link #getSpecializationState} methods must have public
 * no-arg constructor and must be stateless (no fields).
 *
 * @see SpecializationState
 */
public final class SpecializationService
{
  private static final Logger LOG = new Logger(SpecializationService.class);

  private static final Unsafe UNSAFE;
  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException("Cannot access Unsafe methods", e);
    }
  }

  /**
   * If true, specialization is not actually done, an instance of prototypeClass is used as a "specialized" instance.
   * Useful for analysis of generated assembly with JITWatch (https://github.com/AdoptOpenJDK/jitwatch), because
   * JITWatch shows only classes present in the loaded JAR (prototypeClass should be), not classes generated during
   * runtime.
   */
  private static final boolean fakeSpecialize = Boolean.getBoolean("fakeSpecialize");

  /**
   * Number of loop iterations, accounted via {@link SpecializationState#accountLoopIterations(long)} in
   * {@link WindowedLoopIterationCounter} during the last hour window, after which WindowedLoopIterationCounter decides
   * to specialize class for the specific runtimeShape. The default value is chosen to be so that the specialized
   * class will likely be compiled with C2 HotSpot compiler with the default values of *BackEdgeThreshold options.
   */
  private static final int triggerSpecializationIterationsThreshold =
      Integer.getInteger("triggerSpecializationIterationsThreshold", 10_000);

  /**
   * The maximum number of specializations, that this service is allowed to make. It's not unlimited because each
   * specialization takes some JVM memory (machine code cache, byte code, etc.)
   */
  private static final int maxSpecializations = Integer.getInteger("maxSpecializations", 1000);
  private static final AtomicBoolean maxSpecializationsWarningEmitted = new AtomicBoolean(false);

  private static final ExecutorService classSpecializationExecutor = Execs.singleThreaded("class-specialization-%d");

  private static final AtomicLong specializedClassCounter = new AtomicLong();

  private static final ClassValue<PerPrototypeClassState> perPrototypeClassState =
      new ClassValue<PerPrototypeClassState>()
      {
        @Override
        protected PerPrototypeClassState computeValue(Class<?> type)
        {
          return new PerPrototypeClassState<>(type);
        }
      };

  /**
   * @param <T> type of query processing algorithm
   * @see SpecializationService class-level javadoc for details
   */
  public static <T> SpecializationState<T> getSpecializationState(
      Class<? extends T> prototypeClass,
      String runtimeShape
  )
  {
    return getSpecializationState(prototypeClass, runtimeShape, ImmutableMap.of());
  }

  /**
   * @param classRemapping classes, that should be replaced in the bytecode of the given prototypeClass when specialized
   * @see #getSpecializationState(Class, String)
   */
  @SuppressWarnings("unchecked")
  public static <T> SpecializationState<T> getSpecializationState(
      Class<? extends T> prototypeClass,
      String runtimeShape,
      ImmutableMap<Class<?>, Class<?>> classRemapping
  )
  {
    return perPrototypeClassState.get(prototypeClass).getSpecializationState(runtimeShape, classRemapping);
  }

  static class PerPrototypeClassState<T>
  {
    private final Class<T> prototypeClass;
    private final ConcurrentMap<SpecializationId, SpecializationState<T>> specializationStates =
        new ConcurrentHashMap<>();
    private final String prototypeClassBytecodeName;
    private final String specializedClassNamePrefix;

    private byte[] prototypeClassBytecode;

    PerPrototypeClassState(Class<T> prototypeClass)
    {
      this.prototypeClass = prototypeClass;
      String prototypeClassName = prototypeClass.getName();
      prototypeClassBytecodeName = classBytecodeName(prototypeClassName);
      specializedClassNamePrefix = prototypeClassName + "$Copy";
    }

    SpecializationState<T> getSpecializationState(String runtimeShape, ImmutableMap<Class<?>, Class<?>> classRemapping)
    {
      SpecializationId specializationId = new SpecializationId(runtimeShape, classRemapping);
      SpecializationState<T> alreadyExistingState = specializationStates.get(specializationId);
      if (alreadyExistingState != null) {
        return alreadyExistingState;
      }
      return specializationStates.computeIfAbsent(specializationId, id -> new WindowedLoopIterationCounter<>(this, id));
    }

    T specialize(ImmutableMap<Class<?>, Class<?>> classRemapping)
    {
      String specializedClassName = specializedClassNamePrefix + specializedClassCounter.get();
      ClassWriter specializedClassWriter = new ClassWriter(0);
      SimpleRemapper remapper = new SimpleRemapper(createRemapping(classRemapping, specializedClassName));
      ClassVisitor classTransformer = new ClassRemapper(specializedClassWriter, remapper);
      try {
        ClassReader prototypeClassReader = new ClassReader(getPrototypeClassBytecode());
        prototypeClassReader.accept(classTransformer, 0);
        byte[] specializedClassBytecode = specializedClassWriter.toByteArray();
        Class<T> specializedClass = defineClass(specializedClassName, specializedClassBytecode);
        specializedClassCounter.incrementAndGet();
        return specializedClass.newInstance();
      }
      catch (InstantiationException | IllegalAccessException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    private HashMap<String, String> createRemapping(
        ImmutableMap<Class<?>, Class<?>> classRemapping,
        String specializedClassName
    )
    {
      HashMap<String, String> remapping = new HashMap<>();
      remapping.put(prototypeClassBytecodeName, classBytecodeName(specializedClassName));
      for (Map.Entry<Class<?>, Class<?>> classRemappingEntry : classRemapping.entrySet()) {
        Class<?> sourceClass = classRemappingEntry.getKey();
        Class<?> remappingClass = classRemappingEntry.getValue();
        remapping.put(classBytecodeName(sourceClass.getName()), classBytecodeName(remappingClass.getName()));
      }
      return remapping;
    }

    @SuppressWarnings("unchecked")
    private Class<T> defineClass(String specializedClassName, byte[] specializedClassBytecode)
    {
      return (Class<T>) UNSAFE.defineClass(
          specializedClassName,
          specializedClassBytecode,
          0,
          specializedClassBytecode.length,
          prototypeClass.getClassLoader(),
          prototypeClass.getProtectionDomain()
      );
    }

    /**
     * No synchronization, because {@link #specialize} is called only from {@link #classSpecializationExecutor}, i. e.
     * from a single thread.
     */
    byte[] getPrototypeClassBytecode() throws IOException
    {
      if (prototypeClassBytecode == null) {
        ClassLoader cl = prototypeClass.getClassLoader();
        try (InputStream prototypeClassBytecodeStream =
                 cl.getResourceAsStream(prototypeClassBytecodeName + ".class")) {
          prototypeClassBytecode = ByteStreams.toByteArray(prototypeClassBytecodeStream);
        }
      }
      return prototypeClassBytecode;
    }

    private static String classBytecodeName(String className)
    {
      return className.replace('.', '/');
    }
  }

  private static class SpecializationId
  {
    private final String runtimeShape;
    private final ImmutableMap<Class<?>, Class<?>> classRemapping;
    private final int hashCode;

    private SpecializationId(String runtimeShape, ImmutableMap<Class<?>, Class<?>> classRemapping)
    {
      this.runtimeShape = runtimeShape;
      this.classRemapping = classRemapping;
      this.hashCode = runtimeShape.hashCode() * 1000003 + classRemapping.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      if (!(obj instanceof SpecializationId)) {
        return false;
      }
      SpecializationId other = (SpecializationId) obj;
      return runtimeShape.equals(other.runtimeShape) && classRemapping.equals(other.classRemapping);
    }

    @Override
    public int hashCode()
    {
      return hashCode;
    }
  }

  /**
   * Accumulates the number of iterations during the last hour. (Window size = 1 hour)
   */
  static class WindowedLoopIterationCounter<T> extends SpecializationState<T> implements Runnable
  {
    private final PerPrototypeClassState<T> perPrototypeClassState;
    private final SpecializationId specializationId;
    /** A map with the number of iterations per each minute during the last hour */
    private final ConcurrentMap<Long, AtomicLong> perMinuteIterations = new ConcurrentHashMap<>();
    private final AtomicBoolean specializationScheduled = new AtomicBoolean(false);

    WindowedLoopIterationCounter(
        PerPrototypeClassState<T> perPrototypeClassState,
        SpecializationId specializationId
    )
    {
      this.perPrototypeClassState = perPrototypeClassState;
      this.specializationId = specializationId;
    }

    @Nullable
    @Override
    public T getSpecialized()
    {
      // Returns null because the class is not yet specialized. The purpose of WindowedLoopIterationCounter is to decide
      // whether specialization should be done, or not.
      return null;
    }

    @Override
    public void accountLoopIterations(long loopIterations)
    {
      if (specializationScheduled.get()) {
        return;
      }
      if (loopIterations > triggerSpecializationIterationsThreshold ||
          addAndGetTotalIterationsOverTheLastHour(loopIterations) > triggerSpecializationIterationsThreshold) {
        if (specializationScheduled.compareAndSet(false, true)) {
          classSpecializationExecutor.submit(this);
        }
      }
    }

    private long addAndGetTotalIterationsOverTheLastHour(long newIterations)
    {
      long currentMillis = System.currentTimeMillis();
      long currentMinute = TimeUnit.MILLISECONDS.toMinutes(currentMillis);
      long minuteOneHourAgo = currentMinute - TimeUnit.HOURS.toMinutes(1);
      long totalIterations = 0;
      boolean currentMinutePresent = false;
      for (Iterator<Map.Entry<Long, AtomicLong>> it = perMinuteIterations.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<Long, AtomicLong> minuteStats = it.next();
        long minute = minuteStats.getKey();
        if (minute < minuteOneHourAgo) {
          it.remove();
        } else if (minute == currentMinute) {
          totalIterations += minuteStats.getValue().addAndGet(newIterations);
          currentMinutePresent = true;
        } else {
          totalIterations += minuteStats.getValue().get();
        }
      }
      if (!currentMinutePresent) {
        perMinuteIterations.computeIfAbsent(currentMinute, m -> new AtomicLong()).addAndGet(newIterations);
        totalIterations += newIterations;
      }
      return totalIterations;
    }

    @Override
    public void run()
    {
      try {
        T specialized;
        if (specializedClassCounter.get() > maxSpecializations) {
          // Don't specialize, just instantiate the prototype class and emit a warning.
          // The "better" approach is probably to implement some kind of cache eviction from
          // PerPrototypeClassState.specializationStates. But it might be that nobody ever hits even the current
          // maxSpecializations limit, so implementing cache eviction is an unnecessary complexity.
          specialized = perPrototypeClassState.prototypeClass.newInstance();
          if (!maxSpecializationsWarningEmitted.get() && maxSpecializationsWarningEmitted.compareAndSet(false, true)) {
            LOG.warn(
                "SpecializationService couldn't make more than [%d] specializations. " +
                "Not doing specialization for runtime shape[%s] and class remapping[%s], using the prototype class[%s]",
                maxSpecializations,
                specializationId.runtimeShape,
                specializationId.classRemapping,
                perPrototypeClassState.prototypeClass
            );
          }
        } else if (fakeSpecialize) {
          specialized = perPrototypeClassState.prototypeClass.newInstance();
          LOG.info(
              "Not specializing prototype class[%s] for runtime shape[%s] and class remapping[%s] because "
              + "fakeSpecialize=true, using the prototype class instead",
              perPrototypeClassState.prototypeClass,
              specializationId.runtimeShape,
              specializationId.classRemapping
          );
        } else {
          specialized = perPrototypeClassState.specialize(specializationId.classRemapping);
          LOG.info(
              "Specializing prototype class[%s] for runtime shape[%s] and class remapping[%s]",
              perPrototypeClassState.prototypeClass,
              specializationId.runtimeShape,
              specializationId.classRemapping
          );
        }
        perPrototypeClassState.specializationStates.put(specializationId, new Specialized<>(specialized));
      }
      catch (Exception e) {
        LOG.error(
            e,
            "Error specializing prototype class[%s] for runtime shape[%s] and class remapping[%s]",
            perPrototypeClassState.prototypeClass,
            specializationId.runtimeShape,
            specializationId.classRemapping
        );
      }
    }
  }

  static class Specialized<T> extends SpecializationState<T>
  {
    private final T specialized;

    Specialized(T specialized)
    {
      this.specialized = specialized;
    }

    @Override
    public T getSpecialized()
    {
      return specialized;
    }

    @Override
    public void accountLoopIterations(long loopIterations)
    {
      // do nothing
    }
  }

  private SpecializationService()
  {
  }
}
