package mapreduce.topNMix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNMixReducer extends Reducer<TopNMixBean, Text, Text, Text>{

    private Integer topN;

    @Override
    protected void setup(Context context) {
        topN = Integer.parseInt(context.getConfiguration().get("topN"));
    }

    @Override
    protected void reduce(TopNMixBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //        for(int i = 0; i < topN && itr.hasNext(); i++){
        for (Text value : values) {
            context.write(new Text(key.getCourse()), value);
        }
//        }
    }
}
