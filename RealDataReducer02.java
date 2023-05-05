import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class RealDataReducer02 extends Reducer<Text, Text, Text, Text>{
    public void reduce(Text chave, Iterable<Text> valores, Context context) throws IOException, InterruptedException{
        //Converter o Iterable em lista para poder iterar mais de uma vez
        List<Text> values = new ArrayList<Text>();

        for(Text valor : valores){
            values.add(new Text(valor));
        }

        //O número de documentos em que o termo t aparece será o tamanho da lista
        int document_frequency = values.size();

        //Salvar como: termo idDoc_termFrequency_documentFrequency
        for(Text value : values){
            context.write(chave, new Text(value + "_" + document_frequency));
        }
    }
}
