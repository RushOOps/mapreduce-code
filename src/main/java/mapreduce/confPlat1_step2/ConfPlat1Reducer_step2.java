package mapreduce.confPlat1_step2;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.StringUtil;

import java.io.IOException;

public class ConfPlat1Reducer_step2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(key.toString().getBytes().length > 100) {
            context.getCounter("Invalid Data", "Key Too Big").increment(1);
            return;
        }

        JSONObject json = new JSONObject();
        for (Text value : values) {
            String[] macCount = value.toString().split("\\|");
            if(macCount.length < 2 || StringUtil.isEmpty(macCount[0]) || StringUtil.isEmpty(macCount[1])){
                context.getCounter("Invalid Data", "Fatal Error").increment(1);
                return;
            }
            json.compute(macCount[0], (k,v) -> {
                if(v == null){
                    return Integer.parseInt(macCount[1]);
                }else{
                    return (Integer)v+Integer.parseInt(macCount[1]);
                }
            });
        }

        context.write(key, new Text(json.toString()));
    }

}



