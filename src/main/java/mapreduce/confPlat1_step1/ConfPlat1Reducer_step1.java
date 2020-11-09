package mapreduce.confPlat1_step1;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ConfPlat1Reducer_step1 extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, new LongWritable(Iterables.size(values)));
    }

}
