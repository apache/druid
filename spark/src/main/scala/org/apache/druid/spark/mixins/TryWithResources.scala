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

package org.apache.druid.spark.mixins

import scala.util.control.{ControlThrowable, NonFatal}

/**
  * Utility trait to ape the try-with-resources construct in Java. This is quick and dirty and has its flaws. If a more
  * robust approach is needed, the better-files approach (https://github.com/pathikrit/better-files#lightweight-arm)
  * should be considered.
  *
  * Scala doesn't support varargs of generic type, so using overloading to support managing multiple resources with a
  * sequence. Ideally Scala would support macro programming to generate tryWithResources functions for
  * tuples of [_ <: AutoCloseable] so users could un-tuple multiple resources as named variables without needing to
  * manually generate separate methods for each length up to 22, but since it doesn't I've created methods for up to
  * five resources at once. These methods can be nested or the method taking a Sequence of AutoCloseables can be used
  * for arbitrary resources, with the caveat that named unpacking is not possible over Sequences. If there was a way to
  * enforce type bounds on the elements of subclasses of Produce we could hack up a type that unioned AutoCloseable and
  * Products whose elements are all subtypes of AutoCloseable, but since there isn't this is a quick fix. Note that
  * shared code for the tuple methods is not refactored to a single private method because the common supertype of
  * tuples is Product, with no type information.
  */
trait TryWithResources {
  /**
    * A helper function to duplicate Java's try-with-resources construction. Mix in this trait and then call like so:
    * val resource = new ResourceImplementingAutoCloseable()
    * tryWithResources(resource){r =>
    *   r.doSomething()
    * }
    *
    * or, if desired,
    *
    * tryWithResources(new ResourceImplementingAutoCloseable()){ resouce =>
    *   resource.doSomething()
    * }
    *
    * @param resource The AutoCloseable resource to use in FUNC.
    * @param func     The function block to execute (think of this as the try block).
    * @tparam T Any subtype of AutoCloseable.
    * @tparam V The result type of FUNC.
    * @return The result of executing FUNC with RESOURCE.
    */
  def tryWithResources[T <: AutoCloseable, V](resource: T)(func: T => V): V = {
    try {
      func(resource)
    } catch {
      // Clean up for InterruptedExceptions and ControlThrowables, not just NonFatal exceptions
      case e@(NonFatal(_) | _: InterruptedException | _: ControlThrowable) =>
        try {
          resource.close()
        } catch {
          case NonFatal(e2) =>
            // e2 isn't fatal, let the original exception e take precedence
            e.addSuppressed(e2)
          case e2: Throwable =>
            if (NonFatal(e)) {
              // e2 is fatal but e isn't, suppress e
              e2.addSuppressed(e)
              throw e2
            }
            // Both exceptions are fatal, suppress the also-fatal e2 that occurred while closing
            e.addSuppressed(e2)
        }
        throw e
    } finally {
      resource.close()
    }
  }

  /**
    * A helper function to duplicate Java's try-with-resources construction. Unfortunately Scala doesn't support varargs
    * for generic types and I don't feel like writing 22 separate functions, so callers will have to keep track of the
    * exact ordering of elements in the resource sequence or nest the provided tuple methods.
    *
    * To use, mix in this trait and then call like so:
    * val fileResource = new ResourceImplementingAutoCloseable()
    * val writerResource = new OtherAutoCloseable()
    * tryWithResources(Seq(fileResource, writerResource)){resources =>
    *   val file = resources(0)
    *   val writer = resources(1)
    *   writer.write(file, data)
    * }
    *
    * @param resources A list of AutoCloseable resources to use in FUNC.
    * @param func      The function block to execute (think of this as the try block).
    * @tparam T Any subtype of AutoCloseable.
    * @tparam V The result type of FUNC.
    * @return The result of executing FUNC with RESOURCES.
    */
  def tryWithResources[T <: AutoCloseable, V](resources: Seq[T])(func: Seq[T] => V): V = {
    try {
      func(resources)
    } catch {
      // Clean up for InterruptedExceptions and ControlThrowables, not just NonFatal exceptions
      case e@(NonFatal(_) | _: InterruptedException | _: ControlThrowable) =>
        throw closeAllResourcesMergingExceptions(resources, e)
    } finally {
      closeAllResourcesFinally(resources)
    }
  }

  /**
    * A helper function to duplicate Java's try-with-resources construction. Mix in this trait and then call like so:
    * val resources = (new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable())
    * tryWithResources(resources){
    *   case (first, second) =>
    *     first.doSomething()
    *     second.doSomething()
    * }
    *
    * or, if desired,
    *
    * tryWithResources(new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable){
    *   case (first, second) =>
    *     first.doSomething()
    *     second.doSomething
    * }
    *
    * @param resources A tuple of AutoCloseable resources to use in FUNC.
    * @param func      The function block to execute (think of this as the try block).
    * @tparam T Any subtype of AutoCloseable.
    * @tparam V The result type of FUNC.
    * @return The result of executing FUNC with RESOURCES.
    */
  def tryWithResources[T <: AutoCloseable, V](resources: (T, T))(func: ((T, T)) => V): V = {
    val closeableResources = resources.productIterator.toSeq.map(_.asInstanceOf[AutoCloseable])
    try {
      func(resources)
    } catch {
      // Clean up for InterruptedExceptions and ControlThrowables, not just NonFatal exceptions
      case e@(NonFatal(_) | _: InterruptedException | _: ControlThrowable) =>
        throw closeAllResourcesMergingExceptions(closeableResources, e)
    } finally {
      closeAllResourcesFinally(closeableResources)
    }
  }

  /**
    * A helper function to duplicate Java's try-with-resources construction. Mix in this trait and then call like so:
    * val resources = (new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable(), ...)
    * tryWithResources(resources){
    *   case (first, second, ...) =>
    *     first.doSomething()
    *     second.doSomething()
    *     ...
    * }
    *
    * or, if desired,
    *
    * tryWithResources(new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable, ...){
    *   case (first, second, ...) =>
    *     first.doSomething()
    *     second.doSomething()
    *     ...
    * }
    *
    * @param resources A tuple of AutoCloseable resources to use in FUNC.
    * @param func      The function block to execute (think of this as the try block).
    * @tparam T Any subtype of AutoCloseable.
    * @tparam V The result type of FUNC.
    * @return The result of executing FUNC with RESOURCES.
    */
  def tryWithResources[T <: AutoCloseable, V](resources: (T, T, T))(func: ((T, T, T)) => V): V = {
    val closeableResources = resources.productIterator.toSeq.map(_.asInstanceOf[AutoCloseable])
    try {
      func(resources)
    } catch {
      // Clean up for InterruptedExceptions and ControlThrowables, not just NonFatal exceptions
      case e@(NonFatal(_) | _: InterruptedException | _: ControlThrowable) =>
        throw closeAllResourcesMergingExceptions(closeableResources, e)
    } finally {
      closeAllResourcesFinally(closeableResources)
    }
  }

  /**
    * A helper function to duplicate Java's try-with-resources construction. Mix in this trait and then call like so:
    * val resources = (new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable(), ...)
    * tryWithResources(resources){
    *   case (first, second, ...) =>
    *     first.doSomething()
    *     second.doSomething()
    *     ...
    * }
    *
    * or, if desired,
    *
    * tryWithResources(new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable, ...){
    *   case (first, second, ...) =>
    *     first.doSomething()
    *     second.doSomething()
    *     ...
    * }
    *
    * @param resources A tuple of AutoCloseable resources to use in FUNC.
    * @param func      The function block to execute (think of this as the try block).
    * @tparam T Any subtype of AutoCloseable.
    * @tparam V The result type of FUNC.
    * @return The result of executing FUNC with RESOURCES.
    */
  def tryWithResources[T <: AutoCloseable, V](resources: (T, T, T, T))(func: ((T, T, T, T)) => V): V = {
    val closeableResources = resources.productIterator.toSeq.map(_.asInstanceOf[AutoCloseable])
    try {
      func(resources)
    } catch {
      // Clean up for InterruptedExceptions and ControlThrowables, not just NonFatal exceptions
      case e@(NonFatal(_) | _: InterruptedException | _: ControlThrowable) =>
        throw closeAllResourcesMergingExceptions(closeableResources, e)
    } finally {
      closeAllResourcesFinally(closeableResources)
    }
  }

  /**
    * A helper function to duplicate Java's try-with-resources construction. Mix in this trait and then call like so:
    * val resources = (new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable(), ...)
    * tryWithResources(resources){
    *   case (first, second, ...) =>
    *     first.doSomething()
    *     second.doSomething()
    *     ...
    * }
    *
    * or, if desired,
    *
    * tryWithResources(new ResourceImplementingAutoCloseable(), new ResourceImplementingAutoCloseable, ...){
    *   case (first, second, ...) =>
    *     first.doSomething()
    *     second.doSomething()
    *     ...
    * }
    *
    * @param resources A tuple of AutoCloseable resources to use in FUNC.
    * @param func      The function block to execute (think of this as the try block).
    * @tparam T Any subtype of AutoCloseable.
    * @tparam V The result type of FUNC.
    * @return The result of executing FUNC with RESOURCES.
    */
  def tryWithResources[T <: AutoCloseable, V](resources: (T, T, T, T, T))(func: ((T, T, T, T, T)) => V): V = {
    val closeableResources = resources.productIterator.toSeq.map(_.asInstanceOf[AutoCloseable])
    try {
      func(resources)
    } catch {
      // Clean up for InterruptedExceptions and ControlThrowables, not just NonFatal exceptions
      case e@(NonFatal(_) | _: InterruptedException | _: ControlThrowable) =>
        throw closeAllResourcesMergingExceptions(closeableResources, e)
    } finally {
      closeAllResourcesFinally(closeableResources)
    }
  }

  /**
    * Given a list of closeables RESOURCES and an throwable EXCEPTION, close all supplied resources, merging any
    * additional errors that occur. The main point here is to ensure every resource in resources is closed; I'm not sure
    * how useful the "merge" logic actually is when pulled out to a list of closeables instead of just a single one.
    *
    * @param resources The list of resources to close.
    * @param exception The exception to merge additional throwables into.
    * @return The final throwable resulting from "merging" EXCEPTION with any additional throwables raised while closing
    *         the resources in RESOURCES.
    */
  private def closeAllResourcesMergingExceptions(resources: Seq[AutoCloseable], exception: Throwable): Throwable = {
    resources.foldRight(exception)((resource, ex) =>
      try {
        resource.close()
        ex
      } catch {
        case NonFatal(e2) =>
          // e2 isn't fatal, let the original exception e take precedence
          ex.addSuppressed(e2)
          ex
        case e2: Throwable =>
          if (NonFatal(ex)) {
            // e2 is fatal but e isn't, suppress e
            e2.addSuppressed(ex)
            e2
          } else {
            // Both exceptions are fatal, suppress the also-fatal e2 that occurred while closing
            ex.addSuppressed(e2)
            ex
          }
      }
    )
  }

  /**
    * Given RESOURCES, attempts to close all of them even in the face of errors. Arbitrarily, the last exception
    * encountered is thrown, with earlier exceptions suppressed.
    *
    * @param resources The list of resources to close.
    */
  private def closeAllResourcesFinally(resources: Seq[AutoCloseable]): Unit = {
    // Using foldRight to iterate over resources to ensure we don't short circuit and leave resources unclosed if an
    // earlier resource throws an exception on .close().
    val exceptionOption = resources
      .foldRight(None.asInstanceOf[Option[Throwable]])((resource, exOpt) =>
        try {
          resource.close()
          exOpt
        } catch {
          case e: Throwable =>
            Some(exOpt.fold(e) { ex =>
              ex.addSuppressed(e)
              ex
            })
        }
      )
    if (exceptionOption.isDefined) {
      throw exceptionOption.get
    }
  }
}
