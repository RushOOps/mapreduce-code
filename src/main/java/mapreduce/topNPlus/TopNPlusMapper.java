package mapreduce.topNPlus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNPlusMapper extends Mapper<LongWritable, Text, TopNPlusBean, IntWritable>{

    private TreeMap<TopNPlusBean, Integer> treeMap;
    private Integer topN;

    @Override
    protected void setup(Context context) {
        treeMap = new TreeMap<>((o1, o2) -> o2.getAvgScore().compareTo(o1.getAvgScore()));
        topN = Integer.parseInt(context.getConfiguration().get("topN"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(treeMap.size() > topN) treeMap.remove(treeMap.lastKey());
        String[] splits = value.toString().split(",");
        treeMap.put(new TopNPlusBean(splits[0], Integer.parseInt(splits[2])), Integer.parseInt(splits[2]));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = 0; i < topN; i++){
            Map.Entry<TopNPlusBean, Integer> entry = treeMap.pollFirstEntry();
            context.write(entry.getKey(), new IntWritable(entry.getValue()));
        }
    }
}
