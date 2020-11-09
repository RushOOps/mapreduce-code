package mapreduce.semanticMerge;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDBRecordWriter extends RecordWriter<Text, Text> {

    public MongoCollection<Document> collection = null;

    public MongoDBRecordWriter(){

    }
    public MongoDBRecordWriter(TaskAttemptContext context){
        //获取mongodb的连接
        Configuration conf = context.getConfiguration();
        String ip = conf.get("mongo_ip");
        String dbName = conf.get("mongo_db");
        String tableName = conf.get("mongo_table_to");

        // 获取collection
        MongoClient client = new MongoClient(ip,27017);
        collection = client.getDatabase(dbName).getCollection(tableName);
    }

    @Override
    public void write(Text key, Text value) {

        String keyStr = key.toString();
        String[] values = value.toString().split("@@@");
        Bson filter = Filters.and(Filters.eq("query_text", values[0]),
                Filters.eq("domain", values[1]));
        if(keyStr.equals("OBJECT")){
            collection.updateMany(filter, new Document("$set",
                            new Document("semantic", JSONObject.parseObject(values[2]))));
        }else{
            collection.updateMany(filter, new Document("$set",
                    new Document("semantic", JSONArray.parseArray(values[2]))));
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }



}
