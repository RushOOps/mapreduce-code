package mapreduce.domainIntentFrequency;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.StringUtil;

import java.io.IOException;

public class DomainIntentFrequencyReducer extends Reducer<Text, LongWritable, DomainIntent, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        if(StringUtil.isEmpty(key.toString()) || !values.iterator().hasNext()) return;

        JSONObject keyJSON = JSONObject.parseObject(key.toString());
        DomainIntent domainIntent = new DomainIntent(keyJSON.getString("domain"), keyJSON.getString("intent"));

        long sum = 0L;
        for(LongWritable l : values){
            sum += l.get();
        }

        context.write(domainIntent, new LongWritable(sum));

    }

}
