package mapreduce.hotEntity_step2;

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

public class MongoDBRecordWriter extends RecordWriter<Text, Text> {

    public MongoCollection<Document> collection = null;

    public MongoDBRecordWriter(){

    }
    public MongoDBRecordWriter(TaskAttemptContext context){
        //获取mongodb的连接
        Configuration conf = context.getConfiguration();
        String ip = conf.get("mongo_ip");
        String dbName = conf.get("mongo_db_to");
        String tableName = conf.get("mongo_table_to");

        // 获取collection
        MongoClient client = new MongoClient(ip,27017);
        collection = client.getDatabase(dbName).getCollection(tableName);
    }

    @Override
    public void write(Text key, Text value) {

        collection.insertOne(new Document()
                .append("entity", key.toString())
                .append("label_count", JSONObject.parseObject(value.toString())));

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }



}
