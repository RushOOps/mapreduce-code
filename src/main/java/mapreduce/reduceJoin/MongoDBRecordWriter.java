package mapreduce.reduceJoin;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

public class MongoDBRecordWriter extends RecordWriter<NullWritable, Text> {

    MongoClient client = new MongoClient("10.66.188.17", 27017);
    MongoCollection<Document> collection = client.getDatabase("semantic").getCollection("semantic_jointest_out");

    @Override
    public void write(NullWritable key, Text value) {
        JSONObject record = JSONObject.parseObject(value.toString());
        Document doc = new Document();
        doc.put("query_text", record.getString("query_text"));
        doc.put("domain", record.getString("domain"));
        doc.put("intent", record.getString("intent"));
        doc.put("semantic", record.get("semantic"));
        doc.put("content", record.getJSONObject("content"));
        doc.put("count", record.getInteger("count"));
        collection.insertOne(doc);
    }

    @Override
    public void close(TaskAttemptContext context) {
        client.close();
    }
}
