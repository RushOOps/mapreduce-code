package mapreduce.confPlat1_step1;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class ConfPlat1Mapper_step1 extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) {
            context.getCounter("Invalid Data", "Map Input NullValue").increment(1);
            return;
        }
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String queryText = valueJson.getString("query_text");
        String mac = valueJson.getString("query_mac");
        if(StringUtil.isEmpty(queryText)) {
            context.getCounter("Invalid Data", "QueryText Null").increment(1);
            return;
        }
        if(StringUtil.isEmpty(mac)){
            context.getCounter("Invalid Data", "Mac Null").increment(1);
            return;
        }
        context.write(new Text(queryText+"|"+mac), new LongWritable(1));
    }

}
