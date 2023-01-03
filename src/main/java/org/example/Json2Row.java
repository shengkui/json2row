package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Json2Row {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Json2Row");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField("address", DataTypes.StringType, true)
                }
        );
        String jsonStr1 = "{\"id\" : 1, \"name\" : \"Tom\", \"address\" : \"Shanghai\"}";
        String jsonStr2 = "{\"id\" : 2, \"name\" : \"Jack\", \"address\" : \"Beijing\"}";

        //方法一：不能指定schema
        //这种方法有问题
        //RowFactory.create() 的参数不能这样传
        Row row = RowFactory.create(jsonStr1);
        System.out.println(row);

        //方法二：指定Schema
        //这种方法有问题
        //结果多了一层数组：
        // [[{"id" : 1, "name" : "Tom", "address" : "Shanghai"}],
        //  [{"id" : 2, "name" : "Jack", "address" : "Beijing"}]]
        //RowFactory.create() 的参数不能这样传
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(jsonStr1));
        rows.add(RowFactory.create(jsonStr2));
        Row row2 = new GenericRowWithSchema(rows.toArray(), schema);
        System.out.println(row2);
        System.out.println(row2.schema());

        //方法三：通过 RowFactory 构造 Row list，再转换成 Dataset<Row> (DataFrame)
        Row r1 = RowFactory.create(1, "Tom", "Shanghai");
        Row r2 = RowFactory.create(2, "Jack", "Beijing");
        ArrayList<Row> rowList1 = new ArrayList<>();
        rowList1.add(r1);
        rowList1.add(r2);
        Dataset<Row> df1 = spark.createDataFrame(rowList1, schema);
        df1.show();
        df1.printSchema();

        //方法四：从 JSON 直接转换成 Dataset<Row> (DataFrame)
        try (JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())) {
            List<String> rowList2 = new ArrayList<>();
            rowList2.add(jsonStr1);
            rowList2.add(jsonStr2);

            JavaRDD<String> rdd = jsc.parallelize(rowList2);
            Dataset<Row> df2 = spark.read().schema(schema).json(rdd);
            //df2.show();
            //df2.printSchema();

            processResult(df2);
        }
    }

    private static void processResult(Dataset<Row> df) {
        long startTime = System.currentTimeMillis();

        Iterator<Row> it = df.collectAsList().iterator();
        ArrayList<String> jsonList = new ArrayList<>();
        while (it.hasNext()) {
            Row row = it.next();
            String jsonStr = row.json();
            jsonList.add(jsonStr);
        }

        long endTime = System.currentTimeMillis();

        System.out.println("time: " + (endTime - startTime));
        System.out.println("result: " + jsonList);
    }
}
