package mapreduce.musicUnmatch;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class MusicUnmatchMapper extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String domain = valueJson.getString("return_domain");
        String type = valueJson.getString("type");
        JSONObject semantic = valueJson.getJSONObject("return_semantic");
        if(!"MUSIC".equals(domain)
                || !"Unmatch".equals(type)
                || semantic == null
                || semantic.size() == 0) return;
        semantic.forEach((k,v) -> {
            try {
                context.write(new Text(k+"###"+v), new LongWritable(1));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

}
