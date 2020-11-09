//package mapreduce.LyricsMerge;
//
//import com.alibaba.fastjson.JSONObject;
//import com.mongodb.MongoClient;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import mapreduce.semanticMerge.MongoDBInputSplit;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.bson.Document;
//
//import java.io.IOException;
//
//public class MongoDBRecordReader extends RecordReader<Text, IntWritable> {
//
//    mapreduce.semanticMerge.MongoDBInputSplit split;
//    Text key;
//    LongWritable value;
//    MongoClient client;
//    MongoCollection<Document> collection;
//    Long cursorLimit;
//    MongoCursor<Document> cursor;
//    Long splitLength;
//    Long now;
//
//    @Override
//    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        Configuration conf = context.getConfiguration();
//        this.split = (MongoDBInputSplit) split;
//        this.client = new MongoClient(conf.get("mongo_ip"), 27017);
//        this.collection = client.getDatabase(conf.get("mongo_db")).getCollection(conf.get("mongo_collection"));
//        this.cursorLimit = conf.getLong("cursor_limit", 100000);
//        this.splitLength = split.getLength();
//        this.now = 0L;
//        this.cursor = collection.find().limit(new Long(Math.min(cursorLimit, splitLength)).intValue()).cursor();
//    }
//
//    @Override
//    public boolean nextKeyValue() {
//        if(now+1 > splitLength){
//            return false;
//        }
//        if(!cursor.hasNext()){
//            this.cursor = collection.find().skip(now.intValue()).limit(new Long(Math.min(cursorLimit, splitLength-now)).intValue()).cursor();
//        }
//        now++;
//        Document doc = cursor.next();
//        this.key = new Text(doc.getString("lyrics"));
//        this.value = new LongWritable(doc.getLong("count"));
//        return true;
//    }
//
//    @Override
//    public Text getCurrentKey() {
//        return key;
//    }
//
//    @Override
//    public LongWritable getCurrentValue() {
//        return value;
//    }
//
//    @Override
//    public float getProgress() {
//        return now.floatValue()/splitLength.floatValue();
//    }
//
//    @Override
//    public void close() {
//        client.close();
//    }
//}
