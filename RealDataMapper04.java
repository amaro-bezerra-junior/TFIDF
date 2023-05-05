import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RealDataMapper04 extends Mapper<Object, Text, Text, Text>{
    public void map(Object info, Text input, Context context) throws IOException, InterruptedException{
        //Dividir os dados: termo | idDoc | termFrequency | documentFrequency
        String[] dados = input.toString().split("_");

        //Salvar como: idDoc_termo termFrequency_documentFrequency
        context.write(new Text(dados[1] + "_" + dados[0]), new Text(dados[2] + "_" + dados[3]));
    }
}
