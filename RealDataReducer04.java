import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RealDataReducer04 extends Reducer<Text, Text, Text, DoubleWritable>{
    public void reduce(Text chave, Iterable<Text> valores, Context context) throws IOException, InterruptedException{

        //Ler o output do ContadorDeDocs para saber quantos documentos existem no arquivo (N)
        Path path = Paths.get("PathTo/Output03/part-r-00000");
        int qtdDocs = Integer.parseInt(Files.readAllLines(path).get(0).split("\\s")[1]);

        for(Text valor : valores){
            //Dividir os valores: termFrequency | documentFrequency
            String[] dados = valor.toString().split("_");

            //Calcular inverse_document_frequency
            double inverse_document_frequency = Math.log10(qtdDocs / (1+Double.parseDouble(dados[1])));

            //Calcular o TFIDF = term_frequency * inverse_document_frequency
            double tfidf = Integer.parseInt(dados[0]) * inverse_document_frequency;
    
            //Salvar como: idDoc_termo TFIDF
            context.write(chave, new DoubleWritable(tfidf));
        }
    }
}
