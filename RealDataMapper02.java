import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class RealDataMapper02 extends Mapper<Object, Text, Text, Text>{
    public void map(Object info, Text input, Context context) throws IOException, InterruptedException{
        //Separar os dados de entrada
        String[] dados = input.toString().split("_");

        //Salvar como: termo idDoc_termFrequency
        context.write(new Text(dados[1]), new Text(dados[0] + "_" + dados[2]));
    }
}