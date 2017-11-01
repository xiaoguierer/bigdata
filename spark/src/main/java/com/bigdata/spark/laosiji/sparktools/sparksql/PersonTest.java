package com.bigdata.spark.laosiji.sparktools.sparksql;


import com.bigdata.spark.laosiji.sparktools.sparkclient.SparkClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

public class PersonTest {
    SQLContext sqlContext = new SQLContext(SparkClient.getSparkClient("test"));

    JavaRDD<Person> people = SparkClient.getSparkClient("test").textFile("hdfs://master:9000/testFile/people.txt").map(
            new Function<String, Person>() {
                public Person call(String line) throws Exception {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                }
            });
    DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
        //schemaPeople.registerTempTable("people");

    // SQL can be run over RDDs that have been registered as tables.
    DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
        public String call(Row row) {
            return "Name: " + row.getString(0);
        }
    }).collect();

      //  teenagers.persist(StorageLevel.MEMORY_ONLY());
      //  System.out.println(teenagerNames);



    DataFrame df = sqlContext.read().json("hdfs://master:9000/testFile/people.json");
    // Displays the content of the DataFrame to stdout
     //   df.show();
    // age  name
    // null Michael
    // 30   Andy
    // 19   Justin

    // Print the schema in a tree format
       // df.printSchema();
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
        //df.select("name").show();
    // name
    // Michael
    // Andy
    // Justin

    // Select everybody, but increment the age by 1
       // df.select(df.col("name"), df.col("age").plus(1)).show();
    // name    (age + 1)
    // Michael null
    // Andy    31
    // Justin  20

    // Select people older than 21
       // df.filter(df.col("age").gt(21)).show();
    // age name
    // 30  Andy

    // Count people by age
       // df.groupBy("age").count().show();
    // age  count
    // null 1
    // 19   1
    // 30   1
}
