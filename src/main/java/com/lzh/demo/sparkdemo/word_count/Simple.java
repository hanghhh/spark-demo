package com.lzh.demo.sparkdemo.word_count;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import java.util.regex.Pattern;

/**
 * @author liuzihang
 * @description: TODO
 * @date 2021/4/25 6:11 下午
 */
public class Simple {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        String logFile = "/opt/homebrew/Cellar/apache-spark/3.1.1/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();
        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }
}
