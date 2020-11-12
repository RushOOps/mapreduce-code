package mapreduce.mapJoin;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;

public class MongoDBRecordWriter extends RecordWriter<NullWritable, Text> {

    MongoClient client = new MongoClient("10.66.188.17", 27017);
    MongoCollection<Document> collection = client.getDatabase("semantic").getCollection("semantic_jointest_out");

    @Override
    public void write(NullWritable key, Text value) throws IOException, InterruptedException {
        JSONObject valueJson = JSONObject.parseObject(value.toString());
        Document doc = new Document();
        doc.put("query_text", valueJson.getString("query_text"));
        doc.put("domain", valueJson.getString("domain"));
        doc.put("intent", valueJson.getString("intent"));
        doc.put("semantic", valueJson.getJSONObject("semantic"));
        doc.put("content", valueJson.getJSONObject("content"));
        doc.put("count", valueJson.getInteger("count"));
        collection.insertOne(doc);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        client.close();
    }
}
