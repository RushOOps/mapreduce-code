package mapreduce.topNMix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigDecimal;

public class TopNMixMapper extends Mapper<LongWritable, Text, TopNMixBean, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");
        int sum = 0;
        int count = 0;
        for(int i = 2; i < splits.length; i++){
            sum += Integer.parseInt(splits[i]);
            count++;
        }
        double result = 1D * sum/count;
        BigDecimal b = new BigDecimal(result).setScale(1, BigDecimal.ROUND_HALF_UP);
        context.write(new TopNMixBean(splits[0], b.doubleValue()), new Text(splits[1]+"-"+b.toString()));
    }
}
