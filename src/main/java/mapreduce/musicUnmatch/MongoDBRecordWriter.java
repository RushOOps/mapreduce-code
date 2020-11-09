package mapreduce.musicUnmatch;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDBRecordWriter extends RecordWriter<Text, LongWritable> {

    public MongoCollection<Document> collection = null;
    public MongoCollection<Document> collectionHistory = null;

    public MongoDBRecordWriter(){

    }
    public MongoDBRecordWriter(TaskAttemptContext context){
        //获取mongodb的连接
        Configuration conf = context.getConfiguration();
        String ip = conf.get("mongo_ip");
        String dbName = conf.get("mongo_db");
        String tableName = conf.get("mongo_table");
        String tableName_history = conf.get("mongo_table_history");

        // 获取collection
        MongoClient client = new MongoClient(ip,27017);
        collection = client.getDatabase(dbName).getCollection(tableName);
        collectionHistory = client.getDatabase(dbName).getCollection(tableName_history);
    }

    @Override
    public void write(Text key, LongWritable value) {

        String[] keys = key.toString().split("###");

        collection.insertOne(new Document()
                .append("key", keys[0])
                .append("value", keys[1])
                .append("count", value.get()));

        Bson filter = Filters.and(Filters.eq("key", keys[0]), Filters.eq("value", keys[1]));
        Document update = new Document().append("$set", new Document().append("key", keys[0]).append("value", keys[1]))
                .append("$inc", new Document("count", value.get()));
        collectionHistory.findOneAndUpdate(filter, update, new FindOneAndUpdateOptions().upsert(true));
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }

}
