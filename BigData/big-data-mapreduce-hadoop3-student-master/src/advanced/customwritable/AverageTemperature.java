package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/tde.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // registro de classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //definição dos tipos de saida
        //map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);

        //reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //definiçao dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtendo o conteúdo das linhas
            String linha = value.toString();

            // quebrando a linha em campos
            String campos[] = linha.split(";");

            //acessando a posição do pais, ano, flow type, commodity
            String pais = campos[0];
            String ano = campos[1];
            String flow = campos[4];
            String commodityvalue = campos[2];

            //ocorrencia
            int ocorrencia = 1;

            // enviando os dados do map para o sort/shuffle e depois para o reduce
            // Chave comum para todas as ocorrencias, foi preciso criar uma classe pra poder enviar 2 parametros para o write

            // ex.1 - gerando um resultado mais global de acordo com as transacoes do brasil
            //con.write(new Text("BRTransactions"), new FireAvgTempWritable(pais, ocorrencia));

            // ex.2 e 3 - gerando resultado para cada ano
            con.write(new Text(ano), new FireAvgTempWritable(pais, ocorrencia));

            // ex. 3- gerando resultado para flow type
            // con.write(new Text(flow), new FireAvgTempWritable(pais, ocorrencia));

            // ex. 4- gerando resultados para a media de commodities por ano;
            con.write(new Text(commodityvalue), new FireAvgTempWritable(pais, ocorrencia));
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

                // 1. The number of transactions involving Brazil;
                int somaBrasil = 0;
                int somaNs = 0;

                for(FireAvgTempWritable o : values) {
                    if(o.getPais().equals("Brazil")){
                        somaBrasil = somaBrasil + 1;
                    }
                    somaNs += o.getOcorrencia();
                }

                // jogando ocorrencias de resultado no arquivo final

                // exercicio 1
                // con.write(new Text("BRTransactions"), new IntWritable(somaBrasil));

                // exercicio 2 e 3
                // con.write(key, new IntWritable(somaNs));

                // exercicio 3
                // con.write(key, new IntWritable(somaNs));

                // exercio 4

        }
    }
}
