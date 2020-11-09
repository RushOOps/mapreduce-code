package mapreduce.semanticCal;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

public class MongoDBRecordWriter extends RecordWriter<Text, IntWritable> {

    private MongoClient client;
    private MongoCollection<Document> collection;

    public MongoDBRecordWriter(){

    }
    public MongoDBRecordWriter(TaskAttemptContext context){
        //获取mongodb的连接
        Configuration conf = context.getConfiguration();

        // 获取collection
        client = new MongoClient(conf.get("mongo_ip"),27017);
        collection = client.getDatabase(conf.get("mongo_db")).getCollection(conf.get("mongo_collection_to"));
    }

    @Override
    public void write(Text key, IntWritable value) {

        JSONObject keyJson = JSONObject.parseObject(key.toString());

        collection.insertOne(new Document()
                .append("query_text", keyJson.getString("query_text"))
                .append("domain", keyJson.getString("domain"))
                .append("intent", keyJson.getString("intent"))
                .append("semantic", keyJson.getJSONObject("semantic"))
                .append("count", value.get())
        );

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
        client.close();
    }



}
