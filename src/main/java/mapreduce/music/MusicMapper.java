package mapreduce.music;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class MusicMapper extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());
        JSONObject semantic = valueJson.getJSONObject("return_semantic");
        String domain = valueJson.getString("return_domain");

        if(domain == null
                || !domain.equals("MUSIC")
                || semantic == null
                || semantic.size() == 0) return;

        for(String k : semantic.keySet()){
            context.write(new Text(k+"|"+semantic.getString(k)), new LongWritable(1));
        }
    }

}
