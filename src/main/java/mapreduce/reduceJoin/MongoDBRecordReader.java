package mapreduce.reduceJoin;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

public class MongoDBRecordReader extends RecordReader<Text, Text> {

    private Text key;
    private Text value;
    private MongoCursor<Document> cursor;
    private final MongoClient client = new MongoClient("10.66.188.17", 27017);
    private MongoDBInputSplit split;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        split = (MongoDBInputSplit) inputSplit;
        cursor = client.getDatabase("semantic").getCollection(split.getCollection()).find().cursor();
    }

    @Override
    public boolean nextKeyValue() {
        if (cursor.hasNext()){
            Document doc = cursor.next();
            JSONObject key = new JSONObject();
            key.put("query_text", doc.getString("query_text"));
            key.put("domain", doc.getString("domain"));
            this.key = new Text(key.toString());
            JSONObject value = new JSONObject();
            if(split.getCollection().equals("semantic_jointest_01")){
                value.put("intent", doc.getString("intent"));
                value.put("semantic", ((Document)doc.get("semantic")).toJson());
                value.put("count", doc.getInteger("count"));
                value.put("collection", "semantic");
            }else{
                value.put("content", ((Document)doc.get("content")).toJson());
                value.put("collection", "content");
            }
            this.value = new Text(value.toString());
            return true;
        }else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() {
        return this.key;
    }

    @Override
    public Text getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() {
        client.close();
    }
}
