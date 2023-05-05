import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RealDataMapper01 extends Mapper<Object, Text, Text, IntWritable>{
    public void map(Object info, Text input, Context context) throws IOException, InterruptedException{
        
        //Ler e salvar em uma lista o arquivo contendo as stopwords
        List<String> stopwords = Files.readAllLines(Paths.get("PathTo/stop_words_english.txt"));

        //Criar uma regex das stopwords
        String stopWordsRegex = stopwords.stream().collect(Collectors.joining("|", "\\b(", ")\\b\\s?"));

        //Receber os dados de entrada
        String linha = input.toString().toLowerCase().replaceAll(stopWordsRegex, "");

        //Criar o objeto pattern contendo a regex para ler o input
        Pattern mascara = Pattern.compile("([0-9]+),([0-9]+),[\"]*(.*?)[\"]*,[\"]*(.*?)[\"]*$");

        //Criar o objeto reconhecedor
        Matcher matcher = mascara.matcher(linha);

        if(matcher.matches()){
            //Pegar o grupo 4 (corpo da notícia)
            //replaceAll("[^a-z\\s+]", " ") = substitua por espaço tudo o que não for letras do alfabeto nem espaços (caracteres especiais, números, etc.)
            //replaceAll("^[a-z][^a-z]|[^a-z][a-z][^a-z]", " ") = substitua todas as letras sozinhas e iniciais sozinhas por espaço
            //replaceAll("\\s+", " ") = substitua todos os espaços replicados por apenas 1 espaço
            //split("\\s") = salve as palavras em um vetor splitadas por espaço
            String[] termos = matcher.group(4).replaceAll("[^a-z\\s+]", " ").replaceAll("^[a-z][^a-z]|[^a-z][a-z][^a-z]", " ").replaceAll("\\s+", " ").split("\\s");
            
            //Salvar como: idDoc_termo 1
            for(String termo : termos){
                //Se a posição do vetor não estiver vazia ou for nula, salve
                if(!termo.isBlank() && !termo.isEmpty())
                    context.write(new Text(matcher.group(1) + "_" + termo), new IntWritable(1));
            }
        }
    }
}