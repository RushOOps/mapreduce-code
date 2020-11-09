package mapreduce.confPlat0;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

public class MongoDBRecordWriter extends RecordWriter<Text, LongWritable> {

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

        collection.insertOne(new Document().append("mac", key.toString())
                .append("count", value.get()));

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }

}
