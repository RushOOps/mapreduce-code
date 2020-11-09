package mapreduce.errorSuggest2;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class ErrorSuggest2Mapper extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String queryText = valueJson.getString("query_text");
        String intent = valueJson.getString("return_intent");
        if(StringUtil.isEmpty(queryText) || StringUtil.isEmpty(intent)) return;
        if(!queryText.contains("我想听") && !queryText.contains("我要听")) return;
        if(!intent.equals("KTV") && !intent.equals("PLAY")) return;

        JSONObject semantic = valueJson.getJSONObject("return_semantic");
        if(semantic == null || semantic.size() == 0) return;
        String song = semantic.getString("song");
        String singer = semantic.getString("singer");
        if(StringUtil.isNotEmpty(song)){
            context.write(new Text("song|"+semantic.getString("song")), new LongWritable(1));
        }
        if(StringUtil.isNotEmpty(singer)){
            context.write(new Text("singer|"+semantic.getString("singer")), new LongWritable(1));
        }
    }

}
