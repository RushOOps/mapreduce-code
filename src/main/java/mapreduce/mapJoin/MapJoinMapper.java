package mapreduce.mapJoin;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<Text, Text, NullWritable, Text> {

    Map<String, String> map = new HashMap<>();

    @Override
    protected void setup(Context context) {
        MongoClient client = new MongoClient("10.66.188.17", 27017);
        MongoCollection<Document> collection = client.getDatabase("semantic").getCollection("semantic_jointest_02");
        for(Document doc : collection.find()){
            JSONObject keyJson = new JSONObject();
            keyJson.put("query_text", doc.getString("query_text"));
            keyJson.put("domain", doc.getString("domain"));
            map.put(keyJson.toString(), ((Document)doc.get("content")).toJson());
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String mapValue = map.get(key.toString());
        if(mapValue != null){
            JSONObject jsonValue = JSONObject.parseObject(value.toString());
            jsonValue.putAll(JSONObject.parseObject(key.toString()));
            jsonValue.put("content", JSONObject.parseObject(mapValue));
            context.write(NullWritable.get(), new Text(jsonValue.toString()));
        }
    }
}
