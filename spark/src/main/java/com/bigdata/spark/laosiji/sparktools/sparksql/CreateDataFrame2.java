package com.bigdata.spark.laosiji.sparktools.sparksql;

import com.bigdata.spark.laosiji.sparktools.sparkclient.SparkClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;

public class CreateDataFrame2 {
    public static void main(String[] args) {
        SQLContext sqlContext = new SQLContext(SparkClient.getSparkClient("test"));
        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> people = SparkClient.getSparkClient("test").textFile("hdfs://master:9000/testFile/people.txt");

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = people.map(new Function<String, Row>() {
            public Row call(String record) throws Exception {
                String[] fields = record.split(",");
                return RowFactory.create(fields[0], fields[1].trim());
            }
        });

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            // true表示可以为空
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Apply the schema to the RDD.
        DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

        // Register the DataFrame as a table.
        peopleDataFrame.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame results = sqlContext.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal
        // RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> names = results.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        results.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(names);
    }

}
