/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.param

import scala.annotation.varargs
import scala.collection.mutable
import org.apache.spark.annotation.{AlphaComponent}
import java.lang.reflect.Modifier
import java.util.NoSuchElementException
import org.apache.spark.annotation.{DeveloperApi}
import org.apache.spark.ml.Identifiable
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import scala.collection.JavaConverters._

/**
 * :: AlphaComponent ::
 * A param with self-contained documentation and optionally default value. Primitive-typed param
 * should use the specialized versions, which are more friendly to Java users.
 *
 * @param parent parent object
 * @param name param name
 * @param doc documentation
 * @tparam T param value type
 */

@AlphaComponent
class Param[T](val parent: String,
                 val name: String,
                 val doc: String,
                 val isValid: T => Boolean,
                 val defaultValue: Option[T])
    extends Serializable {

    def this(parent: String, name: String, doc: String, isValid: T => Boolean) =
      this(parent: String, name: String,  doc: String, isValid: T => Boolean, None)

    def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
      this(parent.uid, name, doc, isValid)

    def this(parent: String, name: String, doc: String) =
      this(parent, name, doc, ParamValidators.alwaysTrue[T])

    def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

    def this(parent: Params, name: String, doc: String,  defaultValue: Option[T] = None) = this(parent.uid, name, doc,ParamValidators.alwaysTrue[T], defaultValue)

  /**
   * Creates a param pair with the given value (for Java).
   */
  def w(value: T): ParamPair[T] = this -> value

  /**
   * Creates a param pair with the given value (for Scala).
   */
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  override def toString: String = {
    if (defaultValue.isDefined) {
      s"$name: $doc (default: ${defaultValue.get})"
    } else {
      s"$name: $doc"
    }
  }

  /** Check that the array length is greater than lowerBound. */
  def arrayLengthGt[T](lowerBound: Double): Array[T] => Boolean = { (value: Array[T]) =>
    value.length > lowerBound
  }
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Double]]] for Java.
 */
@DeveloperApi
class DoubleParam(parent: String, name: String, doc: String, isValid: Double => Boolean, defaultValue: Option[Double])
  extends Param[Double](parent, name, doc, isValid, defaultValue) {

  def this(parent: String, name: String, doc: String, isValid: Double => Boolean) =
    this(parent: String, name: String,  doc: String, isValid, None)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Double => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  // backward compatibility

  def this(parent: Params, name: String, doc: String, defaultValue: Option[Double]) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue, defaultValue)

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Int]]] for Java.
 */
@DeveloperApi
class IntParam(parent: String, name: String, doc: String, isValid: Int => Boolean, defaultValue: Option[Int])
  extends Param[Int](parent, name, doc, isValid, defaultValue) {

  def this(parent: String, name: String, doc: String, isValid: Int => Boolean) =
    this(parent: String, name: String,  doc: String, isValid, None)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Int => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  // backward compatibility

  def this(parent: Params, name: String, doc: String, defaultValue: Option[Int]) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue, defaultValue)

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Float]]] for Java.
 */
@DeveloperApi
class FloatParam(parent: String, name: String, doc: String, isValid: Float => Boolean, defaultValue: Option[Float])
  extends Param[Float](parent, name, doc, isValid, defaultValue) {

  def this(parent: String, name: String, doc: String, isValid: Float => Boolean) =
    this(parent: String, name: String,  doc: String, isValid, None)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Float => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  // backward compatibility
  def this(parent: Params, name: String, doc: String, defaultValue: Option[Float]) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue, defaultValue)

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Long]]] for Java.
 */
@DeveloperApi
class LongParam(parent: String, name: String, doc: String, isValid: Long => Boolean, defaultValue: Option[Long])
  extends Param[Long](parent, name, doc, isValid, defaultValue) {

  def this(parent: String, name: String, doc: String, isValid: Long => Boolean) =
    this(parent: String, name: String,  doc: String, isValid, None)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Long => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  // backward compatibility
  def this(parent: Params, name: String, doc: String, defaultValue: Option[Long]) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue, defaultValue)

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Boolean]]] for Java.
 */
@DeveloperApi
class BooleanParam(parent: String, name: String, doc: String, isValid: Boolean => Boolean, defaultValue: Option[Boolean]) // No need for isValid
  extends Param[Boolean](parent, name, doc, isValid, defaultValue) {

  def this(parent: String, name: String, doc: String, isValid: Boolean => Boolean) =
    this(parent: String, name: String,  doc: String, isValid, None)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc,ParamValidators.alwaysTrue)

  // backward compatibility
  def this(parent: Params, name: String, doc: String, defaultValue: Option[Boolean]) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue, defaultValue)

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)
}


/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[String]]]] for Java.
 */
@DeveloperApi
class StringArrayParam(parent: Params, name: String, doc: String, isValid: Array[String] => Boolean)
  extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[String]): ParamPair[Array[String]] = w(value.asScala.toArray)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[Double]]]] for Java.
 */
@DeveloperApi
class DoubleArrayParam(parent: Params, name: String, doc: String, isValid: Array[Double] => Boolean)
  extends Param[Array[Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Double]): ParamPair[Array[Double]] =
    w(value.asScala.map(_.asInstanceOf[Double]).toArray)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[Int]]]] for Java.
 */
@DeveloperApi
class IntArrayParam(parent: Params, name: String, doc: String, isValid: Array[Int] => Boolean)
  extends Param[Array[Int]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Integer]): ParamPair[Array[Int]] =
    w(value.asScala.map(_.asInstanceOf[Int]).toArray)
}


/**
 * A param amd its value.
 */
case class ParamPair[T](param: Param[T], value: T)

/**
 * :: AlphaComponent ::
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
 */
@AlphaComponent
trait Params extends Identifiable with Serializable {

  /** Returns all params. */
  def params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
        Modifier.isPublic(m.getModifiers) &&
          classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
          m.getParameterTypes.isEmpty
      }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /**
   * Validates parameter values stored internally plus the input parameter map.
   * Raises an exception if any parameter is invalid.
   */
  def validate(paramMap: ParamMap): Unit = {}

  /**
   * Validates parameter values stored internally.
   * Raise an exception if any parameter value is invalid.
   */
  def validate(): Unit = validate(ParamMap.empty)

  /**
   * Returns the documentation of all params.
   */
  def explainParams(): String = params.mkString("\n")

  /** Checks whether a param is explicitly set. */
  def isSet(param: Param[_]): Boolean = {
    require(param.parent.eq(this.uid))
    paramMap.contains(param)
  }

  /** Tests whether this instance contains a param with a given name. */
  def hasParam(paramName: String): Boolean = {
    params.exists(_.name == paramName)
  }

  /** Gets a param by its name. */
  private[ml] def getParam(paramName: String): Param[Any] = {
    val m = this.getClass.getMethod(paramName)
    assert(Modifier.isPublic(m.getModifiers) &&
      classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
      m.getParameterTypes.isEmpty)
    m.invoke(this).asInstanceOf[Param[Any]]
  }

  /**
   * Sets a parameter in the embedded param map.
   */
  protected def set[T](param: Param[T], value: T): this.type = {
    require(param.parent.eq(this.uid))
    paramMap.put(param.asInstanceOf[Param[Any]], value)
    this
  }

  /**
   * Sets a parameter (by name) in the embedded param map.
   */
  private[ml] def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
  }

  /**
   * Gets the value of a parameter in the embedded param map.
   */
  protected def get[T](param: Param[T]): T = {
    getOrDefault(param)
//    require(param.parent.eq(this.uid))
//    paramMap(param)
  }

  //  /** An alias for [[getOrDefault()]]. */
  protected final def $[T](param: Param[T]): T = getOrDefault(param)


  /**
   * Gets the value of a param in the embedded param map or its default value. Throws an exception
   * if neither is set.
   */
  final def getOrDefault[T](param: Param[T]): T = {
    shouldOwn(param)
    //TODO need to double check. even the paramMap contains the param, it does not mean it always to get the right default, may need to indicator to check the default map
    if(paramMap.contains(param))
      paramMap(param)
    else
      getDefault(param).get
  }

  /**
   * Sets a default value for a param.
   * @param param  param to set the default value. Make sure that this param is initialized before
   *               this method gets called.
   * @param value  the default value
   */
  protected final def setDefault[T](param: Param[T], value: T): this.type = {
    defaultParamMap.put(param -> value)
    this
  }

  /** Validates that the input param belongs to this instance. */
  private def shouldOwn(param: Param[_]): Unit = {
    //require(param.parent == uid && hasParam(param.name), s"Param $param does not belong to $this.")
  }

  /**
   * Sets default values for a list of params.
   *
   * @param paramPairs  a list of param pairs that specify params and their default values to set
   *                    respectively. Make sure that the params are initialized before this method
   *                    gets called.
   */
  @varargs
  protected final def setDefault(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      setDefault(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /**
   * Gets the default value of a parameter.
   */
  final def getDefault[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    defaultParamMap.get(param)
  }

  /**
   * Tests whether the input param has a default value set.
   */
  final def hasDefault[T](param: Param[T]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param)
  }

  /**
   * Default implementation of copy with extra params.
   * It tries to create a new instance with the same UID.
   * Then it copies the embedded and extra parameters over and returns the new instance.
   */
  protected final def defaultCopy[T <: Params](extra: ParamMap): T = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyValues(that, extra).asInstanceOf[T]
  }

  /**
   * Copies param values from this instance to another instance for params shared by them.
   * @param to the target instance
   * @param extra extra params to be copied
   * @return the target instance with param values copied
   */
  protected def copyValues[T <: Params](to: T, extra: ParamMap = ParamMap.empty): T = {
    val map = extractParamMap(extra)
    params.foreach { param =>
      if (map.contains(param) && to.hasParam(param.name)) {
        to.set(param.name, map(param))
      }
    }
    to
  }

  /**
   * Extracts the embedded default param values and user-supplied values, and then merges them with
   * extra values from input into a flat param map, where the latter value is used if there exist
   * conflicts, i.e., with ordering: default param values < user-supplied values < extra.
   */
  final def extractParamMap(extra: ParamMap): ParamMap = {
    defaultParamMap ++ paramMap ++ extra
  }

  /**
   * [[extractParamMap]] with no extra values.
   */
  final def extractParamMap(): ParamMap = {
    extractParamMap(ParamMap.empty)
  }

  /**
   * Internal param map.
   */
  protected val paramMap: ParamMap = ParamMap.empty

  /** Internal param map for default values. */
  private val defaultParamMap: ParamMap = ParamMap.empty

  /**
   * Check whether the given schema contains an input column.
   * @param colName  Parameter name for the input column.
   * @param dataType  SQL DataType of the input column.
   */
  protected def checkInputColumn(schema: StructType, colName: String, dataType: DataType): Unit = {
    val actualDataType = schema(colName).dataType
    require(actualDataType.equals(dataType),
      s"Input column $colName must be of type $dataType" +
        s" but was actually $actualDataType.  Column param description: ${getParam(colName)}")
  }

  protected def addOutputColumn(
      schema: StructType,
      colName: String,
      dataType: DataType): StructType = {
    if (colName.length == 0) return schema
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(colName), s"Prediction column $colName already exists.")
    val outputFields = schema.fields ++ Seq(StructField(colName, dataType, nullable = false))
    StructType(outputFields)
  }
}

/**
 * :: DeveloperApi ::
 *
 * Helper functionality for developers.
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@DeveloperApi
private[spark] object Params {

  /**
   * Copies parameter values from the parent estimator to the child model it produced.
   * @param paramMap the param map that holds parameters of the parent
   * @param parent the parent estimator
   * @param child the child model
   */
  def inheritValues[E <: Params, M <: E](
      paramMap: ParamMap,
      parent: E,
      child: M): Unit = {
    parent.params.foreach { param =>
      if (paramMap.contains(param)) {
        child.set(child.getParam(param.name), paramMap(param))
      }
    }
  }
}

/**
 * :: AlphaComponent ::
 * A param to value map.
 */
@AlphaComponent
class ParamMap private[ml] (private val map: mutable.Map[Param[Any], Any]) extends Serializable {

  /**
   * Creates an empty param map.
   */
  def this() = this(mutable.Map.empty[Param[Any], Any])

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   */
  def put[T](param: Param[T], value: T): this.type = {
    map(param.asInstanceOf[Param[Any]]) = value
    this
  }

  /**
   * Puts a list of param pairs (overwrites if the input params exists).
   */
  @varargs
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      put(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /**
   * Optionally returns the value associated with a param or its default.
   */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]])
      .orElse(param.defaultValue)
      .asInstanceOf[Option[T]]
  }

  /**
   * Gets the value of the input param or its default value if it does not exist.
   * Raises a NoSuchElementException if there is no value associated with the input param.
   */
  def apply[T](param: Param[T]): T = {
    val value = get(param)
    if (value.isDefined) {
      value.get
    } else {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }

  /**
   * Checks whether a parameter is explicitly specified.
   */
  def contains(param: Param[_]): Boolean = {
    map.contains(param.asInstanceOf[Param[Any]])
  }

  /**
   * Filters this param map for the given parent.
   */
  def filter(parent: Params): ParamMap = {
    val filtered = map.filterKeys(_.parent == parent)
    new ParamMap(filtered.asInstanceOf[mutable.Map[Param[Any], Any]])
  }

  /**
   * Make a copy of this param map.
   */
  def copy: ParamMap = new ParamMap(map.clone())

  override def toString: String = {
    map.toSeq.sortBy(_._1.name).map { case (param, value) =>
      s"\t${param.parent}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exists conflicts.
   */
  def ++(other: ParamMap): ParamMap = {
    // TODO: Provide a better method name for Java users.
    new ParamMap(this.map ++ other.map)
  }

  /**
   * Adds all parameters from the input param map into this param map.
   */
  def ++=(other: ParamMap): this.type = {
    // TODO: Provide a better method name for Java users.
    this.map ++= other.map
    this
  }

  /**
   * Converts this param map to a sequence of param pairs.
   */
  def toSeq: Seq[ParamPair[_]] = {
    map.toSeq.map { case (param, value) =>
      ParamPair(param, value)
    }
  }

  /**
   * Number of param pairs in this set.
   */
  def size: Int = map.size
}

object ParamMap {

  /**
   * Returns an empty param map.
   */
  def empty: ParamMap = new ParamMap()

  /**
   * Constructs a param map by specifying its entries.
   */
  @varargs
  def apply(paramPairs: ParamPair[_]*): ParamMap = {
    new ParamMap().put(paramPairs: _*)
  }
}


/**
 * :: DeveloperApi ::
 * Factory methods for common validation functions for [[Param.isValid]].
 * The numerical methods only support Int, Long, Float, and Double.
 */
@DeveloperApi
object ParamValidators {

  /** (private[param]) Default validation always return true */
  private[ml] def alwaysTrue[T]: T => Boolean = (_: T) => true

  /**
   * Private method for checking numerical types and converting to Double.
   * This is mainly for the sake of compilation; type checks are really handled
   * by [[Params]] setters and the [[ParamPair]] constructor.
   */
  private def getDouble[T](value: T): Double = value match {
    case x: Int => x.toDouble
    case x: Long => x.toDouble
    case x: Float => x.toDouble
    case x: Double => x.toDouble
    case _ =>
      // The type should be checked before this is ever called.
      throw new IllegalArgumentException("Numerical Param validation failed because" +
        s" of unexpected input type: ${value.getClass}")
  }

  /** Check if value > lowerBound */
  def gt[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) > lowerBound
  }

  /** Check if value >= lowerBound */
  def gtEq[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) >= lowerBound
  }

  /** Check if value < upperBound */
  def lt[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) < upperBound
  }

  /** Check if value <= upperBound */
  def ltEq[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) <= upperBound
  }

  /**
   * Check for value in range lowerBound to upperBound.
   * @param lowerInclusive  If true, check for value >= lowerBound.
   *                        If false, check for value > lowerBound.
   * @param upperInclusive  If true, check for value <= upperBound.
   *                        If false, check for value < upperBound.
   */
  def inRange[T](
                  lowerBound: Double,
                  upperBound: Double,
                  lowerInclusive: Boolean,
                  upperInclusive: Boolean): T => Boolean = { (value: T) =>
    val x: Double = getDouble(value)
    val lowerValid = if (lowerInclusive) x >= lowerBound else x > lowerBound
    val upperValid = if (upperInclusive) x <= upperBound else x < upperBound
    lowerValid && upperValid
  }

  /** Version of [[inRange()]] which uses inclusive be default: [lowerBound, upperBound] */
  def inRange[T](lowerBound: Double, upperBound: Double): T => Boolean = {
    inRange[T](lowerBound, upperBound, lowerInclusive = true, upperInclusive = true)
  }

  /** Check for value in an allowed set of values. */
  def inArray[T](allowed: Array[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** Check for value in an allowed set of values. */
  def inArray[T](allowed: java.util.List[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }


  /** Private method for checking array types and converting to Array. */
  private def getArray[T](value: T): Array[_] = value match {
    case x: Array[_] => x
    case _ =>
      // The type should be checked before this is ever called.
      throw new IllegalArgumentException("Array Param validation failed because" +
        s" of unexpected input type: ${value.getClass}")
  }

  /** Check that the array length is greater than lowerBound. */
  def arrayLengthGt[T](lowerBound: Double): Array[T] => Boolean = { (value: Array[T]) =>
    value.length > lowerBound
  }

}