package com.metamx.druid.query;

import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class ReflectionLoaderThingy<T>
{
  private static final Logger log = new Logger(ReflectionLoaderThingy.class);

  public static <K> ReflectionLoaderThingy<K> create(Class<K> interfaceClass)
  {
    return new ReflectionLoaderThingy<K>(interfaceClass);
  }

  Map<Class<?>, AtomicReference<T>> toolChestMap = Maps.newConcurrentMap();

  private final Class<T> interfaceClass;

  public ReflectionLoaderThingy(
      Class<T> interfaceClass
  )
  {
    this.interfaceClass = interfaceClass;
  }

  public T getForObject(Object keyObject)
  {
    Class<?> clazz = keyObject.getClass();

    AtomicReference<T> retVal = toolChestMap.get(clazz);

    if (retVal == null) {
      String interfaceName = interfaceClass.getSimpleName();

      AtomicReference<T> retVal1;
      try {
        final Class<?> queryToolChestClass = Class.forName(String.format("%s%s", clazz.getName(), interfaceName));
        retVal1 = new AtomicReference<T>(interfaceClass.cast(queryToolChestClass.newInstance()));
      }
      catch (Exception e) {
        log.warn(e, "Unable to load interface[%s] for input class[%s]", interfaceClass, clazz);
        retVal1 = new AtomicReference<T>(null);
      }
      retVal = retVal1;

      toolChestMap.put(clazz, retVal);
    }

    return retVal.get();
  }
}
