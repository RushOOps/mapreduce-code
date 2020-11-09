package mapreduce.confPlat0;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class ConfPlat0Mapper extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String mac = valueJson.getString("query_mac");
        if(StringUtil.isEmpty(mac)) return;
        context.write(new Text(mac), new LongWritable(1));
    }

}
