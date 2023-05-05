import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RealDataReducer01 extends Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
        int term_frequency = 0;

        //Contar o número de vezes que o termo t aparece no documento d
        for(IntWritable valor : valores){
           term_frequency += valor.get();
        }

        //Salvar como: idDoc_termo termFrequency
        context.write(chave, new IntWritable(term_frequency));
    }
}
