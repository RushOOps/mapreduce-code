package mapreduce.domainIntentFrequency;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class DomainIntentFrequencyMapper extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(StringUtil.isEmpty(value.toString())) return;

        // 取出domain、intent
        JSONObject valueJson = JSONObject.parseObject(value.toString());
        String domain = valueJson.getString("return_domain");
        String intent = valueJson.getString("return_intent");
        JSONObject outKey = new JSONObject();
        outKey.put("domain", domain);
        outKey.put("intent", intent);

        if(StringUtil.isEmpty(domain) || StringUtil.isEmpty(intent)) return;

        context.write(new Text(outKey.toString()), new LongWritable(1));
    }
}
