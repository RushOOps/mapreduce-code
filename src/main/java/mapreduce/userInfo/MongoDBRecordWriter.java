package mapreduce.userInfo;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Iterator;

public class MongoDBRecordWriter extends RecordWriter<Text, Text> {

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
    public void write(Text key, Text value) {

        String mac = key.toString();
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String lastTime = valueJson.getString("last_time");
        String firstTime = valueJson.getString("first_time");
        Iterator<Document> target = collection.find(new Document("mac", mac)).iterator();
        if(target.hasNext()){
            Document targetDoc = target.next();
            if(firstTime.compareTo(targetDoc.getString("first_time")) > 0){
                firstTime = targetDoc.getString("first_time");
            }
            if(targetDoc.getString("last_time").compareTo(lastTime) > 0){
                lastTime = targetDoc.getString("last_time");
            }
        }
        Bson filters = Filters.eq("mac", mac);
        Document update = new Document()
                .append("$set", new Document()
                        .append("mac", mac)
                        .append("ip", valueJson.getString("ip"))
                        .append("user_id", valueJson.getString("user_id"))
                        .append("device_id", valueJson.getString("device_id"))
                        .append("client_type", valueJson.getString("client_type"))
                        .append("provider_code", valueJson.getString("provider_code"))
                        .append("sversion", valueJson.getString("sversion"))
                        .append("latitude", valueJson.getString("latitude"))
                        .append("longitude", valueJson.getString("longitude"))
                        .append("city_location", valueJson.getString("city_location"))
                        .append("last_time", lastTime)
                        .append("first_time", firstTime))
                .append("$inc", new Document("count", valueJson.getLongValue("count")));

        collection.findOneAndUpdate(filters, update, new FindOneAndUpdateOptions().upsert(true));

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }

}
