package mapreduce.TopNPlus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TopNPlusReducer extends Reducer<TopNPlusBean, IntWritable, Text, IntWritable>{

    private Integer topN;

    @Override
    protected void setup(Context context) {
        topN = Integer.parseInt(context.getConfiguration().get("topN"));
    }

    @Override
    protected void reduce(TopNPlusBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> itr = values.iterator();
        for(int i = 0; i < topN && itr.hasNext(); i++){
            context.write(new Text(key.getCourse()), itr.next());
        }
    }
}
