package mapreduce.groupSort;

import flowBean.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupSortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] intPairStringArr = value.toString().split(" ");
        IntPair intPair = new IntPair(Integer.parseInt(intPairStringArr[0]), Integer.parseInt(intPairStringArr[1]));
        context.write(intPair, new IntWritable(Integer.parseInt(intPairStringArr[1])));
    }
}
