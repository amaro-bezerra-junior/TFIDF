import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RealDataMain01{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        //Salvar como: idDoc_termo_termFrequency
        conf.set(TextOutputFormat.SEPARATOR, "_");
        Job job = Job.getInstance(conf, "ToyData01");

        job.setMapperClass(RealDataMapper01.class);
        job.setReducerClass(RealDataReducer01.class);
        job.setJarByClass(RealDataMain01.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //System.exit(job.waitForCompletion(true)? 0:1);
        job.waitForCompletion(true);
    }
}