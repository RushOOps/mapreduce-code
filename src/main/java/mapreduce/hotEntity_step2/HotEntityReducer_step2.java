package mapreduce.hotEntity_step2;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HotEntityReducer_step2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        JSONObject singleJson;
        JSONObject totalJson = new JSONObject();
        for (Text value : values) {
            singleJson = JSONObject.parseObject(value.toString());
            singleJson.forEach((k,v) -> {
                if(totalJson.containsKey(k)){
                    totalJson.put(k, totalJson.getLongValue(k)+((Integer)v).longValue());
                }else{
                    totalJson.put(k, v);
                }
            });
        }
        context.write(key, new Text(totalJson.toString()));
    }

}



