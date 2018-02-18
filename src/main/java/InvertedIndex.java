
import javafx.util.Pair;
import org.apache.spark.api.java.*;
import org.apache.spark.*;
import scala.Tuple2;
import java.util.Arrays;
import java.io.*;
import java.util.*;

/**
 * Este programa foi desenvolvido como trabalho final
 * da disciplina de IPPD na UFPel
 *
 * Esta aplicação cria um arquivo de Índice Invertido utilizando o Spark
 * executando em um grande conjunto de dados (arquivos texto)
 *
 * @author  Renata Zottis Junges
 * @author  Yan Ballinhas Soares
 * @version 1.0
 * @since   2018-02-18
 */

public class InvertedIndex {

    /**
     * Este método é usado para ler um arquivo e aplicar o MapReduce para
     * fazer a contagem do número de vezes que aparece cada palavra no texto
     *
     * @param path Este é o caminho onde está armazenado o arquivo para leitura
     * @param sc   Este é o contexto do Spark para aplicar as operações de MapReduce
     * @return Tuple2 Retorna uma tupla da forma (nome arquivo, JavaPairRDD(palavra, contagem))
     */
    public static Tuple2<String, JavaPairRDD<String, Integer>>  readFilesSpark (String path, JavaSparkContext sc){

        JavaRDD <String> file = sc.textFile(path);

        JavaPairRDD<String, Integer> counts = file
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        Tuple2<String, JavaPairRDD<String, Integer>> fileCount= new Tuple2(path, counts);

        return fileCount;

    }

    /**
     * Este é o método principal do programa.
     *
     * Este método escreve em um arquivo auxiliar o nome de
     * todos documentos texto com suas respectivas palavras e ocorrências.
     * Após isso, se faz a leitura deste documento e cria-se o Índice Invertido
     * fazendo uma ordenação pelo documento que tem o
     * maior número de ocorrência de determinada palavra.
     *
     * @param args
     */

    public static void main(String[] args) {

        String path = "../dataset/250MB-1/"; //Caminho onde estão os arquivos texto
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        BufferedWriter escrita = null;

        try{
             escrita = new BufferedWriter(new FileWriter("auxiliar.txt"));
        }
        catch(IOException e){}

        //Configurações Spark
        SparkConf conf = new SparkConf().setAppName("MapReduceIPPD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Tuple2<String, JavaPairRDD<String, Integer>> fileCount;
        List<Tuple2<String, Integer>> contagens;

        //Percorre todos os arquivos da pasta para criar a tupla (nome arquivo, JavaPairRDD(palavra, contagem))
        for (int i = 0; i < listOfFiles.length; i++) {

            if (listOfFiles[i].isFile()) {
                String filePath = path + listOfFiles[i].getName();
                fileCount = readFilesSpark(filePath, sc);

                //Grava em um arquivo(auxiliar.txt) as informações de cada arquivo lido
                //Posteriormente esta informação será utilizada para criar o Índice Invertido
                try{
                    escrita.write("~~" + fileCount._1); //nome do arquivo com o ~~ como "identificador" de arquivos
                    escrita.write("\n");
                    contagens = fileCount._2.collect(); //coloca todos os elementos do JavaPairRDD em uma lista

                    //Percorre a lista do JavaPairRDD para escreve-la no arquivo
                    for(int j = 0; j < contagens.size(); j++){
                        escrita.write(contagens.get(j)._1); //escreve a palavra
                        escrita.write(" ");
                        escrita.write(contagens.get(j)._2.toString()); //escreve a ocorrência da palavra
                        escrita.write("\n");
                    }

                }catch(IOException e){}

            }
        }

        //Fecha adequadamente o arquivo de escrita
        try{
            escrita.close();
        }catch(IOException e){}

        /*Começa aqui o arquivo índice invertido*/
        HashMap<String, ArrayList<Pair<String,Integer>>> indiceInvertido = new HashMap<>();
        BufferedReader leitura = null;
        Integer contagem = 0;
        String arquivo = "", linha = "", palavra = "";

        try{
            leitura = new BufferedReader(new FileReader("auxiliar.txt")); //Leitura do arquivo auxiliar

            while((linha = leitura.readLine()) != null){ //lê até o fim do arquivo

                if(linha.startsWith("~")) { //se a linha começa com ~ então é nome do arquivo
                    arquivo = linha.replace("~~", ""); //deixa arquivo como o nome do arquivo atual
                    linha = leitura.readLine(); //pega a primeira palavra deste arquivo e contagem
                }

                palavra = linha.split(" ")[0]; //pega a palavra
                contagem = Integer.parseInt(linha.split(" ")[1]); //pega a contagem da palavra
                Pair<String, Integer> entrada = new Pair<>(arquivo, contagem); //cria a entrada com o par (arquivo, contagem)

                //se a palavra ainda não está no mapa, cria-se uma lista vazia para colocar como valor
                if(!indiceInvertido.containsKey(palavra)){
                    ArrayList<Pair<String, Integer>> listaVazia = new ArrayList<>();
                    listaVazia.add(entrada);
                    indiceInvertido.put(palavra, listaVazia);
                }
                else{
                    //se já tiver uma lista com algum conteúdo para aquela palavra, só adiciona a nova entrada na lista
                    ArrayList<Pair<String,Integer>> lista = indiceInvertido.get(palavra);
                    lista.add(entrada); //adiciona na lista o novo arquivo e o numero de vezes que a palavra aparece
                    indiceInvertido.put(palavra, lista); //armazena a lista modificada no indice invertido
                }
            }
        }catch(IOException e){}

        /* Ordenação do índice invertido de acordo com o número de ocorrências no documento */
        for(String chave: indiceInvertido.keySet()){
            indiceInvertido.get(chave).sort(new Comparator<Pair<String,Integer>>(){
                @Override
                public int compare(Pair<String,Integer> p1, Pair<String,Integer> p2){
                    if(p1.getValue() > p2.getValue()){
                        return -1;
                    } else if(p1.getValue().equals(p2.getValue())){
                        return 0;
                    } else {
                        return 1;
                    }

                }
            });
        }

        /* Escrita no arquivo final (arquivo: indiceInvertido.txt) */
        try{
            escrita = new BufferedWriter(new FileWriter("indiceInvertido.txt"));
            //Para cada palavra do índice invertido escreve a palavra e os documentos com o número de ocorrências
            for (String chave: indiceInvertido.keySet()) {
                escrita.write(chave);
                escrita.write("\n\t");
                List<Pair<String, Integer>> lista = indiceInvertido.get(chave);

                for (Pair<String, Integer> p : lista) {
                    escrita.write(p.getKey());
                    escrita.write("\n\t");
                    escrita.write(p.getValue().toString());
                    escrita.write("\n\t");
                }
                escrita.write("\n");
            }
            escrita.close();
        }
        catch(IOException e){}

    }

}
