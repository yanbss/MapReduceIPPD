import org.apache.commons.math.stat.descriptive.summary.Sum;
import org.apache.spark.api.java.*;
import org.apache.spark.*;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.io.File;

import static org.apache.avro.SchemaBuilder.map;


public class InvertedIndex {

    public static Tuple2<String, JavaPairRDD<String, Integer>>  readFilesSpark (String path, JavaSparkContext sc){

        JavaRDD <String> file = sc.textFile(path);

        JavaPairRDD<String, Integer> counts = file
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        Tuple2<String, JavaPairRDD<String, Integer>> fileCount= new Tuple2(path, counts);
        //System.out.println("Nome arquivo:" + fileCount._1);
        //JavaPairRDD<String, Integer> teste = fileCount._2;
        //teste.foreach(x -> System.out.println("Contagem:" + x));

        return fileCount;

    }

    public static void main(String[] args) {

        String path = "../dataset/MeusTestes/";
        File folder = new File (path);
        File[] listOfFiles = folder.listFiles();

        SparkConf conf = new SparkConf().setAppName("MapReduceIPPD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Tuple2<String, JavaPairRDD<String, Integer>> fileCount;

        for (int i = 0; i < listOfFiles.length; i++){
            if(listOfFiles[i].isFile()){
                String filePath = path+listOfFiles[i].getName();
                fileCount = readFilesSpark(filePath, sc);
                //Cada fileCount é os dados referentes a um dos arquivos, armazenar ele em algum lugar
            }
        }

    }

}
