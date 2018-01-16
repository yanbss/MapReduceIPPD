import javafx.util.Pair;
import org.apache.commons.math.stat.descriptive.summary.Sum;
import org.apache.spark.api.java.*;
import org.apache.spark.*;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.io.*;
import java.util.*;
import javafx.*;

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

        String path = "/home/yan/Área de Trabalho/textos/";
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        BufferedWriter escrita = null;

        try{
             escrita = new BufferedWriter(new FileWriter("teste.txt"));
        }
        catch(IOException e){}

        SparkConf conf = new SparkConf().setAppName("MapReduceIPPD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Tuple2<String, JavaPairRDD<String, Integer>> fileCount;
        List<Tuple2<String, Integer>> contagens;

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                String filePath = path + listOfFiles[i].getName();
                fileCount = readFilesSpark(filePath, sc);
                try{
                    escrita.write(fileCount._1); //nome do arquivo
                    escrita.write("\n");
                    //escrita.write(fileCount._2.toString()); testei só com os do twitter, mas eu acho que não funciona... resolvi garantir fazendo o que tá abaixo:
                    contagens = fileCount._2.collect(); //bota todos os elementos do JavaPairRDD em uma lista pra poder printar
                    for(int j = 0; j < contagens.size(); j++){
                        escrita.write(contagens.get(j)._1); //escreve a palavra
                        escrita.write(" ");
                        escrita.write(contagens.get(j)._2.toString()); //escreve o numero de vezes que a palavra aparece
                        escrita.write("\n");
                    }
                    //contagens.clear(); aparentemente não precisa disso, pq dá que esse tipo de lista não aceita clear (ver isso direitinho pra ver se não da problema)
                }catch(IOException e){}
                //Cada fileCount é os dados referentes a um dos arquivos, armazenar ele em algum lugar
            }
        }

        try{
            escrita.close();
        }catch(IOException e){}

        //Agora para a criação de fato do arquivo de índice invertido:
        //A gente lê desse arquivo que foi gerado na etapa anterior e cria um mapa ou uma tabela
        //com cada palavra relacionada com quais arquivos aparece e o numero de vezes
        //eu acredito que dê pra fazer com um HashMap<String, List<Tuple2<String, Integer>>

        HashMap<String, List<Pair<String,String>>> indiceInvertido = null;
        BufferedReader leitura = null;
        String arquivo = "", linha = "", palavra = "", contagem = "";
        try{
            leitura = new BufferedReader(new FileReader("teste.txt"));
            while((linha = leitura.readLine()) != null){ //lê até o fim do texto
                if(linha.startsWith("/")) { //se a linha começa com / então é nome do arquivo
                    arquivo = linha; //deixa arquivo como o nome do arquivo atual
                    linha = leitura.readLine(); //pega a primeira palavra e contagem desse arquivo
                }
                palavra = linha.split(" ")[0]; //split quebra a linha em um array de strings
                contagem = linha.split(" ")[1];
                Pair<String,String> entrada = new Pair(arquivo, contagem);
                if(!indiceInvertido.containsKey(palavra)){ //se a palavra ainda não está no mapa, tem que criar uma lista vazia para botar como valor
                    List<Pair<String, String>> listaVazia = null;
                    listaVazia.add(entrada);
                    indiceInvertido.put(palavra, listaVazia);
                }
                else{ //se já tiver uma lista com algum conteúdo para aquela palavra, só adiciona a nova entrada na lista
                    //TO DO
                }
            }
        }catch(IOException e){}



    }

}
