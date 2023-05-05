import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RealDataReducer05 extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
    public void reduce(DoubleWritable chave, Iterable<Text> dados, Context context) throws IOException, InterruptedException{
        for(Text dado : dados){
            //Salvar como: idDoc_termo TFIDF
            context.write(dado, chave);
        }
    }
}
