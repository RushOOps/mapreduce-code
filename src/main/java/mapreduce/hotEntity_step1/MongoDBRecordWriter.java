package mapreduce.hotEntity_step1;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public class MongoDBRecordWriter extends RecordWriter<Text, LongWritable> {

    private final static Map<String, String> MAP = new HashMap<String, String>(){
        {
            put("actor", "figure###actor");
            put("director", "figure###director");
            put("writer", "figure###writer");
            put("singer", "figure###singer");
            put("sportsman", "figure###sportsman");
        }
    };

    public MongoCollection<Document> collection = null;

    public MongoDBRecordWriter(){

    }
    public MongoDBRecordWriter(TaskAttemptContext context){
        //获取mongodb的连接
        Configuration conf = context.getConfiguration();
        String ip = conf.get("mongo_ip");
        String dbName = conf.get("mongo_db");
        String tableName = conf.get("mongo_table");

        // 获取collection
        MongoClient client = new MongoClient(ip,27017);
        collection = client.getDatabase(dbName).getCollection(tableName);
    }

    @Override
    public void write(Text key, LongWritable value) {

        String[] keys = key.toString().split("\\|");
        if(MAP.containsKey(keys[0])) keys[0] = MAP.get(keys[0]);
        JSONObject labelCount = new JSONObject();
        labelCount.put(keys[0], value.get());
        collection.insertOne(new Document()
                .append("entity", keys[1])
                .append("label_count", labelCount));

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }



}
