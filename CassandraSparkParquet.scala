package com.yugabyte.sample.apps

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Name(id: Int, name: String)

object CassandraSparkParquet {

  val DEFAULT_KEYSPACE = "ybdemo";
  val PARQUET_INPUT_TABLENAME = "names"

  // Setup the local spark master, with the desired parallelism.
  val conf =
    new SparkConf()
      .setAppName("yb.parquet")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")

  val spark =
    SparkSession.builder
      .config(conf)
      .getOrCreate

  // Create the Spark context object.
  val sc = spark.sparkContext

  // Create the Cassandra connector to Spark.
  val connector = CassandraConnector.apply(conf)

  // Create a Cassandra session, and initialize the keyspace.
  val session = connector.openSession

  //------------ Setting Input source (Cassandra table only) -------------\\

  val inputTable = DEFAULT_KEYSPACE + "." + PARQUET_INPUT_TABLENAME

  // Drop the sample table if it already exists.
  session.execute(s"DROP TABLE IF EXISTS ${inputTable};")

  // Create the input table.
  session.execute(
    s"""
        CREATE TABLE IF NOT EXISTS ${inputTable} (
          id INT,
          name text,
          PRIMARY KEY(id)
        );
      """
  )

  // Generate a parquet.
  import spark.implicits._
  sc.parallelize(List(Name(1, "Apple"), Name(2, "Mango"))).toDF().repartition(1).write.parquet("names")

  // Read data from Parquet file.
  val toInsert = spark.read.parquet("names")

  // Insert DF to cassandra.
  toInsert.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> DEFAULT_KEYSPACE, "table" -> PARQUET_INPUT_TABLENAME))
    .save()

  // Read data from cassandra.
  val df = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> DEFAULT_KEYSPACE, "table" -> PARQUET_INPUT_TABLENAME)).load()

  // Write to Parquet.
  df.repartition(1).write.parquet("new-names")

} // object CassandraSparkParquet
