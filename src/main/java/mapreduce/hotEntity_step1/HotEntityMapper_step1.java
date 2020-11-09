package mapreduce.hotEntity_step1;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class HotEntityMapper_step1 extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String queryText = valueJson.getString("query_text");
        if(StringUtil.isEmpty(queryText)) return;

        context.write(new Text(queryText), new LongWritable(1));

    }

}
