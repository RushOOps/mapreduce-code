package mapreduce.groupSort;

import flowBean.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class GroupSortReducer extends Reducer<IntPair, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> valueItr = values.iterator();
        for(int i = 0; i < 3; i++){
            context.write(new IntWritable(key.getFirst()), valueItr.next());
        }
    }
}
