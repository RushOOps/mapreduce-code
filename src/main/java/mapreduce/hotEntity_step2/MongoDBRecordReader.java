package mapreduce.hotEntity_step2;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.Set;

public class MongoDBRecordReader extends RecordReader<Text, Text> {

    private Text key;
    private Text value;
    private MongoCursor<Document> cursor;
    private long pos;
    private final MongoDBInputSplit split;
    private final MongoCollection<Document> collection;

    public MongoDBRecordReader(MongoDBInputSplit split, TaskAttemptContext context){
        this.split = split;
        Configuration conf = context.getConfiguration();
        MongoClient client = new MongoClient(conf.get("mongo_ip"),27017);
        this.collection = client.getDatabase(
                conf.get("mongo_db_from")).getCollection(conf.get("mongo_table_from"));

    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() {
        if (key == null) {
            key = new Text();
        }
        if (value == null) {
            value = new Text();
        }
        if (cursor == null) {
            cursor = collection.find(new Document()).skip((int) split.getStart()).limit((int) split.getLength()).iterator();
        }
        if (!cursor.hasNext()) {
            return false;
        }

        Document doc = cursor.next();
        key.set(doc.getString("entity"));
        Document valueDoc = (Document)doc.get("label_count");
        Set<String> keySet = valueDoc.keySet();
        JSONObject valueJson = new JSONObject();
        for (String s : keySet){
            valueJson.put(s, valueDoc.getLong(s));
        }
        value.set(valueJson.toString());
        pos++;
        return true;
    }

    @Override
    public Text getCurrentKey() {
        return this.key;
    }

    @Override
    public Text getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() {
        return this.pos;
    }

    @Override
    public void close() {
    }
}
