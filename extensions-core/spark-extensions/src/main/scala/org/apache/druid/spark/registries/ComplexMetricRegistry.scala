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

package org.apache.druid.spark.registries

import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.quantiles.DoublesSketch
import org.apache.datasketches.tuple.ArrayOfDoublesSketch
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule
import org.apache.druid.query.aggregation.datasketches.theta.{SketchHolder, SketchModule}
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule
import org.apache.druid.query.aggregation.histogram.{ApproximateHistogram,
  ApproximateHistogramDruidModule, FixedBucketsHistogram, FixedBucketsHistogramAggregator}
import org.apache.druid.query.aggregation.variance.{VarianceAggregatorCollector, VarianceSerde}
import org.apache.druid.segment.serde.ComplexMetrics
import org.apache.druid.spark.mixins.Logging

import scala.collection.mutable

/**
 * A registry for plugging in support for Druid complex metric types. Provides definitions for supporting complex types
 * in extensions-core out of the box.
 */
object ComplexMetricRegistry extends Logging {
  private val registeredSerdeFunctions: mutable.HashMap[String, () => Unit] = new mutable.HashMap()
  private val registeredSerializeFunctions: mutable.HashMap[Class[_], Any => Array[Byte]] =
    new mutable.HashMap()

  /**
    * Register a function REGISTERSERDEFUNC that initializes serializers and deserializers for the complex metric with
    * the type name NAME. Assumes that the associated complex metric is serialized as a byte array.
    *
    * @param name The type name of the complex metric to register.
    * @param registerSerdeFunc The function to use to register the necessary serdes for this complex metric type.
    */
  def register(
                name: String,
                registerSerdeFunc: () => Unit
              ): Unit = {
    registeredSerdeFunctions(name) = registerSerdeFunc
  }

  /**
    * Register a function REGISTERSERDEFUNC that initializes serializers and deserializers, a class DESERIALIZEDCLASS
    * to deserialize byte arrays into, and a function SERIALIZEFUNC for converting from instances of DESERIALIZEDCLASS
    * to byte arrays for the complex metric with the type name NAME. Assumes that the associated complex metric is
    * serialized as a byte array.
    *
    * @param name The type name of the complex metric to register.
    * @param registerSerdeFunc The function to use to register the necessary serdes for this complex metric type.
    * @param deserializedClass The class to deserialize complex metrics of the registered type to from byte arrays.
    * @param serializeFunc The function to use when serializing instances of DESERIALIZEDCLASS to byte arrays.
    */
  def register(
                name: String,
                registerSerdeFunc: () => Unit,
                deserializedClass: Class[_],
                serializeFunc: Any => Array[Byte]): Unit = {
    registeredSerdeFunctions(name) = registerSerdeFunc
    registeredSerializeFunctions(deserializedClass) = serializeFunc
  }

  /**
    * Shortcut for registering known complex metric serdes (e.g. those in extensions-core) by name.
    *
    * @param name The type name of the complex metric to register.
    * @param shouldCompact Whether or not to store compacted versions of this complex metric. Ignored for complex metric
   *                      type that don't have compacted forms.
    */
  def registerByName(name: String, shouldCompact: Boolean = false): Unit = {
    if (!registeredSerdeFunctions.contains(name) && knownMetrics.contains(name)) {
      knownMetrics(name)(shouldCompact)
    }
  }

  def getRegisteredMetricNames: Set[String] = {
    registeredSerdeFunctions.keySet.toSet
  }

  def getRegisteredSerializedClasses: Set[Class[_]] = {
    registeredSerializeFunctions.keySet.toSet
  }

  def registerSerde(serdeName: String): Unit = {
    if (registeredSerdeFunctions.contains(serdeName)) {
      registeredSerdeFunctions(serdeName).apply()
    }
  }

  def deserialize(col: Any): Array[Byte] = {
    if (registeredSerializeFunctions.keySet.contains(col.getClass)) {
      registeredSerializeFunctions(col.getClass)(col)
    } else {
      throw new IllegalArgumentException(
        s"Unsure how to parse ${col.getClass.toString} into a ByteArray!"
      )
    }
  }

  def registerSerdes(): Unit = {
    registeredSerdeFunctions.foreach(_._2.apply())
  }

  /**
    * Register metric serdes for all complex metric types in extensions-core.
    */
  def initializeDefaults(shouldCompact: Boolean = false): Unit = {
    knownMetrics.foreach(_._2.apply(shouldCompact))
  }

  private val knownMetrics: Map[String, Boolean => Unit] = Map[String, Boolean => Unit](
    // Approximate Histograms
    "approximateHistogram" -> ((_: Boolean) =>
      register(
        "approximateHistogram",
        () => ApproximateHistogramDruidModule.registerSerde(),
        classOf[ApproximateHistogram],
        histogram => histogram.asInstanceOf[ApproximateHistogram].toBytes
      )),
    // Fixed Bucket Histograms
    FixedBucketsHistogramAggregator.TYPE_NAME -> ((_: Boolean) =>
        register(
        FixedBucketsHistogramAggregator.TYPE_NAME,
        () => ApproximateHistogramDruidModule.registerSerde(),
        classOf[FixedBucketsHistogram],
        histogram => histogram.asInstanceOf[FixedBucketsHistogram].toBytes
      )),
    // Tuple Sketches
    ArrayOfDoublesSketchModule.ARRAY_OF_DOUBLES_SKETCH -> ((_: Boolean) =>
      register(
          ArrayOfDoublesSketchModule.ARRAY_OF_DOUBLES_SKETCH,
          // TODO: This probably needs to be wrapped in a try to ensure it only happens once
          () => new ArrayOfDoublesSketchModule().configure(null), // scalastyle:ignore null
          classOf[ArrayOfDoublesSketch],
          sketch => sketch.asInstanceOf[ArrayOfDoublesSketch].toByteArray
      )),
    // Quantiles Sketches
    DoublesSketchModule.DOUBLES_SKETCH -> ((shouldCompact: Boolean) =>
      register(
        DoublesSketchModule.DOUBLES_SKETCH,
        () => DoublesSketchModule.registerSerde(),
        classOf[DoublesSketch],
        sketch => {
          val doublesSketch = sketch.asInstanceOf[DoublesSketch]
          if (shouldCompact) doublesSketch.toByteArray(shouldCompact) else doublesSketch.toByteArray
        }
      )),
    // HLL Sketches
    HllSketchModule.TYPE_NAME -> ((shouldCompact: Boolean) =>
      register(
        HllSketchModule.TYPE_NAME,
        () => HllSketchModule.registerSerde(),
        classOf[HllSketch],
        sketch => {
          val hllSketch = sketch.asInstanceOf[HllSketch]
          if (shouldCompact) hllSketch.toCompactByteArray else hllSketch.toUpdatableByteArray
        }
      )),
    // Theta Sketches
    SketchModule.THETA_SKETCH -> ((shouldCompact: Boolean) =>
      register(
        SketchModule.THETA_SKETCH,
        () => SketchModule.registerSerde(),
        classOf[SketchHolder],
        sketch => {
          val thetaSketch = sketch.asInstanceOf[SketchHolder].getSketch
          if (shouldCompact) thetaSketch.compact().toByteArray else thetaSketch.toByteArray
        }
      )),
    SketchModule.THETA_SKETCH_BUILD_AGG -> ((shouldCompact: Boolean) =>
      register(
        SketchModule.THETA_SKETCH_BUILD_AGG,
        () => SketchModule.registerSerde(),
        classOf[SketchHolder], // TODO: Maybe?
        sketch => {
          val thetaSketch = sketch.asInstanceOf[SketchHolder].getSketch
          if (shouldCompact) thetaSketch.compact().toByteArray else thetaSketch.toByteArray
        }
      )),
    SketchModule.THETA_SKETCH_MERGE_AGG -> ((shouldCompact: Boolean) =>
      register(
        SketchModule.THETA_SKETCH_MERGE_AGG,
        () => SketchModule.registerSerde(),
        classOf[SketchHolder], // TODO: Maybe?
        sketch => {
          val thetaSketch = sketch.asInstanceOf[SketchHolder].getSketch
          if (shouldCompact) thetaSketch.compact().toByteArray else thetaSketch.toByteArray
        }
      )),
    // Variance
    "variance" -> ((_: Boolean) =>
      register(
        "variance",
        () => ComplexMetrics.registerSerde("variance", new VarianceSerde()),
        classOf[VarianceAggregatorCollector],
        collector => collector.asInstanceOf[VarianceAggregatorCollector].toByteArray
      ))
  )
}
