package mapreduce.semanticMerge;

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
import java.util.ArrayList;
import java.util.List;

public class MongoDBRecordReader extends RecordReader<Text, Text> {

    private Text key;
    private Text value;
    private MongoCursor<Document> cursor;
    private long pos;
    private long now;
    private final MongoDBInputSplit split;
    private final MongoCollection<Document> collection;
    private final static List<String> DOMAIN_LIST = new ArrayList<String>(){
        {
            add("WEATHER");
            add("NUMBER");
            add("FOOD");
            add("PhoneCall");
            add("MUSIC");
            add("TIME");
            add("RECIPE");
            add("CONSTELLATION");
        }
    };

    public MongoDBRecordReader(MongoDBInputSplit split, TaskAttemptContext context){
        this.split = split;
        Configuration conf = context.getConfiguration();
        MongoClient client = new MongoClient(conf.get("mongo_ip"),27017);
        this.collection = client.getDatabase(
                conf.get("mongo_db")).getCollection(conf.get("mongo_table_from"));
        now = split.getStart();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {

    }

    @Override
    public boolean nextKeyValue() {
        if (cursor == null) {
            cursor = collection.find(new Document())
                    .skip((int) split.getStart())
                    .limit((int)((split.getEnd()-now)>100000?100000:(split.getEnd()-now))).iterator();
            now += 100000;
        }
        if (overLastRecord()) return false;

        Document doc = cursor.next();
        while(!DOMAIN_LIST.contains(doc.getString("domain"))){
            if (overLastRecord()) return false;
            doc = cursor.next();
        }
        key = new Text(doc.getString("query_text")+"@@@"+doc.getString("domain"));
        value = new Text(((Document)doc.get("semantic")).toJson());
        pos++;
        return true;
    }

    private boolean overLastRecord() {
        if (!cursor.hasNext()) {
            if(now > split.getEnd()){
                return true;
            }else{
                cursor = collection.find(new Document())
                        .skip((int) now)
                        .limit((int)((split.getEnd()-now)>100000?100000:(split.getEnd()-now))).iterator();
                now += 100000;
            }
        }
        return false;
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
