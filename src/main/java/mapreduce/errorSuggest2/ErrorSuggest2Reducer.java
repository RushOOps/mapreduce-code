package mapreduce.errorSuggest2;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ErrorSuggest2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, new LongWritable(Iterables.size(values)));
    }

}
