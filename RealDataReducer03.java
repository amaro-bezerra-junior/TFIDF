import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RealDataReducer03 extends Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> documentos, Context context) throws IOException, InterruptedException{
        
        int qtdDocs = 0;

        for (IntWritable documento : documentos) {
            qtdDocs += documento.get();
        }
        
        context.write(new Text("numeroDeDocs"), new IntWritable(qtdDocs));
    }
}
