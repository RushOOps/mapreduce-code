package mapreduce.reduceJoin;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<JSONObject> collectionSemantic = new ArrayList<>();
        List<JSONObject> collectionContent = new ArrayList<>();

        for(Text value : values){
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            if(valueJson.getString("collection").equals("semantic")){
                valueJson.remove("collection");
                collectionSemantic.add(valueJson);
            }else{
                valueJson.remove("collection");
                collectionContent.add(valueJson);
            }
        }
        if(collectionContent.size() == 0 || collectionSemantic.size() == 0) return;
        JSONObject out = collectionContent.get(0);
        JSONObject keyJson = JSONObject.parseObject(key.toString());
        out.put("query_text", keyJson.getString("query_text"));
        out.put("domain", keyJson.getString("domain"));
        if(collectionSemantic.size() > 1){
            int count = 0;
            JSONArray semanticArr = new JSONArray();
            for(JSONObject s: collectionSemantic){
                count += s.getInteger("count");
                semanticArr.add(s.getJSONObject("semantic"));
            }
            out.put("semantic", semanticArr);
            out.put("count", count);
        }else{
            out.put("semantic", collectionSemantic.get(0).getJSONObject("semantic"));
            out.put("count", collectionSemantic.get(0).getInteger("count"));
        }
        out.put("intent", collectionSemantic.get(0).getString("intent"));
        context.write(NullWritable.get(), new Text(out.toString()));
    }
}
