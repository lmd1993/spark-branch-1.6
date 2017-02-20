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
package org.apache.spark.examples.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.asterix._
import org.apache.asterix.connector._

object ExampleConnector {
  var sc: SparkContext = null

  val aqlQuery = """
                   |let $exampleSet := [
                   | {"name" : "Ann", "age" : 20, "salary" : 100000},
                   | {"name" : "Bob", "age" : 30, "salary" : 200000},
                   | {"name" : "Cat", "age" : 40, "salary" : 300000, "dependents" : [1, 2, 3]}
                   |]
                   |for $x in $exampleSet
                   |return $x
                   |""".stripMargin

  val sqlppQuery = """
                     | SELECT element exampleSet
                     | FROM [
                     | {"name" : "Ann", "age" : 20, "salary" : 100000},
                     | {"name" : "Bob", "age" : 30, "salary" : 200000},
                     | {"name" : "Cat", "age" : 40, "salary" : 300000, "dependents" : [1, 2, 3]}
                     | ] as exampleSet;
                     | """.stripMargin






  def main (args: Array[String]): Unit = {
    /**
      * Configure Spark with AsterixDB-Spark connector configurations.
      */
    val conf = new SparkConf()
      .setMaster("local[4]")
      .set("spark.asterix.connection.host", "localhost") // AsterixDB API host
      .set("spark.asterix.connection.port", "19002") // AsterixDB API port
      .set("spark.asterix.frame.size", "32768") // AsterixDB configured frame size
      .set("spark.driver.memory", "1g")
      .setAppName("AsterixDB-Spark Connector Example")

    // Initialize SparkContext with AsterixDB configuration
    sc = new SparkContext(conf)
    val sqlppContext = new SQLContext(sc)
    val rddAql = sc.aql(aqlQuery)
    val dfSqlpp = sqlppContext.sqlpp(sqlppQuery)
    dfSqlpp.filter(dfSqlpp("age")>30).show()

    // scalastyle:off
    println("AQL result")
    rddAql.collect().foreach(println)
    // scalastyle:on
    // show all dataset and dataverse

    val sqlContext = new SQLContext(sc)
    val dataAll = sqlContext.showAll()
    dataAll.show()
    // choose one dataset in a dataverse that you want to use
    val Dataverse = "tpcds3";
    val Dataset = "catalog_sales"
    val schema = sqlContext.showSchema(Dataverse, Dataset);
    // scalastyle:off
    println(schema)
    // scalastyle:on
    // use one dataset in a dataverse. Return a Dataframe
    val datasetR = sqlContext.useDataset(Dataverse, Dataset).source("AsterixDB", Dataverse, "inventory")
    // datasetR.show()
    // datasetR.filter(datasetR("inv_quantity_on_hand") > 800).show()
    // datasetR.filter(datasetR("inv_quantity_on_hand") > 800).
     // filter(datasetR("inv_quantity_on_hand") > 700).show()
    /*
     * use tpcds3;
     * select count(*)  from (select * from inventory where inv_quantity_on_hand > 700) m1 where inv_quantity_on_hand >800;
     * This is the sql++ version
     */
     val DatasetC = "catalog_sales"
    val datasetT = sqlContext.useDataset(Dataverse, "catalog_sales").source("AsterixDB", Dataverse, DatasetC)
    datasetR.filter(datasetR("inv_item_sk") === "38").
      join(datasetT, datasetR("inv_item_sk") === datasetT("cs_item_sk")).select("inv_item_sk", "inv_date_sk","cs_item_sk").show()

    val a = datasetR.filter(datasetR("inv_item_sk") === "38").
      join(datasetT, datasetR("inv_item_sk") === datasetT("cs_item_sk")).
      count()
    // scalastyle:off
    println(a)
    // scalastyle:on

    /*
    val sqlContext = new SQLContext(sc)

    /* Get DataFrame by running SQL++ query (AQL also supported by calling aql())
     * infer = true means that we tell AsterixDB to provide Spark the result schema.
     * if that throws an exception, probably you AsterixDB doesn't have the schema inferencer.
     * Therefore, let infer = false and Spark will do the job (with the cost of additional scan).
     */
    val dfSqlpp = sqlContext.sqlpp(sqlppQuery, infer = true)
    // scalastyle:off
    println("SQL++ DataFrame result")
    // scalastyle:on
    dfSqlpp.printSchema()
    dfSqlpp.show()
    */
    sc.stop()
  }

}
