import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RealDataMapper03 extends Mapper<Object, Text, Text, IntWritable>{
    public void map(Object info, Text input, Context context) throws IOException, InterruptedException{
        context.write(new Text("documento"), new IntWritable(1));
    }
}
