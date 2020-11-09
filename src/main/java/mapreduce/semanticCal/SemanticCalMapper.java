package mapreduce.semanticCal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SemanticCalMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

        context.write(key, value);

    }

}
