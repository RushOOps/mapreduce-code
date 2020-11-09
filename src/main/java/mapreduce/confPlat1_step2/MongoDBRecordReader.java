package mapreduce.confPlat1_step2;

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

public class MongoDBRecordReader extends RecordReader<Text, Text> {

    private Text key;
    private Text value;
    private MongoCursor<Document> cursor;
    private long pos;
    private long now;
    private final MongoDBInputSplit split;
    private final MongoCollection<Document> collection;

    public MongoDBRecordReader(MongoDBInputSplit split, TaskAttemptContext context){
        this.split = split;
        Configuration conf = context.getConfiguration();
        MongoClient client = new MongoClient(conf.get("mongo_ip"),27017);
        this.collection = client.getDatabase(
                conf.get("mongo_db")).getCollection(conf.get("mongo_table_from"));
        now = split.getStart();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() {
        if (cursor == null) {
            cursor = collection.find(new Document())
                    .skip((int) split.getStart())
                    .limit((int)((split.getEnd()-now)>100000?100000:(split.getEnd()-now))).iterator();
            now += 100000;
        }
        if (!cursor.hasNext()) {
            if(now > split.getEnd()){
                return false;
            }else{
                cursor = collection.find(new Document())
                        .skip((int) now)
                        .limit((int)((split.getEnd()-now)>100000?100000:(split.getEnd()-now))).iterator();
                now += 100000;
            }
        }

        Document doc = cursor.next();
        key = new Text(doc.getString("query_text"));
        value = new Text(doc.getString("mac")+"|"+doc.getLong("count"));
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
