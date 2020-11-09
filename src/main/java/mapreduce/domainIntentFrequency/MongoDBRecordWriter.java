package mapreduce.domainIntentFrequency;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDBRecordWriter extends RecordWriter<DomainIntent, LongWritable> {

    public MongoCollection<Document> domainIntentCollection = null;

    public MongoDBRecordWriter(){

    }
    public MongoDBRecordWriter(TaskAttemptContext context){
        //获取mongodb的连接
        Configuration conf = context.getConfiguration();
        String ip = conf.get("mongo_ip");
        String dbName = conf.get("mongo_db");
        String domainIntentTableName = conf.get("mongo_domain_intent_table");

        // 获取collection
        MongoClient client = new MongoClient(ip,27017);
        domainIntentCollection = client.getDatabase(dbName).getCollection(domainIntentTableName);
    }

    @Override
    public void write(DomainIntent key, LongWritable value) {
        if(key == null || value == null) return;

        // domainIntent表
        Bson filters = Filters.and(Filters.eq("domain", key.getDomain()),
                Filters.eq("intent", key.getIntent()));
        Document update = new Document()
                .append("$set", new Document()
                        .append("domain", key.getDomain())
                        .append("intent", key.getIntent()))
                .append("$inc", new Document("count", value.get()));
        domainIntentCollection.findOneAndUpdate(filters, update, new FindOneAndUpdateOptions().upsert(true));

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {

    }

}
