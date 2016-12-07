package org.template

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        val features: Array[Int] = properties.get[Array[Int]]("features")
        val categories: Array[Int] = properties.get[Array[Int]]("categories")
        val sizes: Array[Int] = properties.get[Array[Int]]("sizes")
        val price: Int = properties.get[Int]("price")
        val original_price: Int = properties.get[Int]("original_price")

        Item(entityId, price, original_price, features, categories, sizes)
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    new TrainingData(items = itemsRDD)
  }
}

case class Item(item: String, price: Int, original_price: Int,
                    features: Array[Int], categories: Array[Int], sizes: Array[Int])

class TrainingData(val items: RDD[(String, Item)]) extends Serializable {
  override def toString = {
    s"items: [${items.count()}] (${items.take(2).toList}...)"
  }
}