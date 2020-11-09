package mapreduce.semanticCal;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import flowBean.MongoDBInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;

public class MongoDBRecordReader extends RecordReader<Text, IntWritable> {

    private MongoClient client;
    private MongoCollection<Document> collection;
    private Text key;
    private IntWritable value;
    private MongoCursor<Document> cursor;
    private MongoDBInputSplit split;
    private Integer length;
    private Integer now;
    private int pos = 0;

    @Override
    public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.client = new MongoClient(conf.get("mongo_ip"), 27017);
        this.collection = client.getDatabase(conf.get("mongo_db")).getCollection(conf.get("mongo_collection"));
        this.split = (MongoDBInputSplit) inputsplit;
        this.length = (int)split.getLength();
        this.now = (int)split.getStart();
    }

    @Override
    public boolean nextKeyValue() {
        if(cursor == null){
            cursor = collection.find().skip((int)split.getStart())
                    .limit(100000<length?100000:length).cursor();
            now += 100000;
        }
        if(!cursor.hasNext()){
            if(now > split.getEnd()) return false;
            cursor = collection.find().skip(now)
                    .limit(100000<split.getEnd()-now ? 100000 : (int)split.getEnd()-now).cursor();
            now += 100000;
        }
        Document doc = cursor.next();
        JSONObject jsonKey = new JSONObject();
        jsonKey.put("query_text", doc.getString("query_text"));
        jsonKey.put("domain", doc.getString("domain"));
        jsonKey.put("intent", doc.getString("intent"));
        jsonKey.put("semantic", ((Document)doc.get("semantic")).toJson());
        key = new Text(jsonKey.toString());
        value = new IntWritable(doc.getInteger("count"));
        pos++;
        return true;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return (float)pos/split.getLength();
    }

    @Override
    public void close() {
        client.close();
    }
}
