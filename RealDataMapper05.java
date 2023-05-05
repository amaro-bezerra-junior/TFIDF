import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RealDataMapper05 extends Mapper<Object, Text, DoubleWritable, Text>{
    public void map(Object info, Text input, Context context) throws IOException, InterruptedException{
        //Como o dado chega:  000001_disappointed;3.102662341897148
        String[] dados = input.toString().split(";");
        //dados[0] = 000001_disappointed  |  dados[1] = 3.102662341897148

        //Passar para o shuffle como:  3.102662341897148 000001_disappointed
        context.write(new DoubleWritable(Double.parseDouble(dados[1])), new Text(dados[0]));
    }
}
