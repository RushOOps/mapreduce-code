//package mapreduce.LyricsMerge;
//
//import com.mongodb.MongoClient;
//import com.mongodb.client.MongoCollection;
//import mapreduce.MongoDBRecordReader;
//import mapreduce.semanticMerge.MongoDBInputSplit;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.*;
//import org.bson.Document;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class MongoDBInputFormat extends InputFormat<Text, IntWritable> {
//
//    @Override
//    public List<InputSplit> getSplits(JobContext context) {
//        Configuration conf = context.getConfiguration();
//        MongoClient client = new MongoClient(conf.get("mongo_ip"), 27017);
//        MongoCollection<Document> collection = client
//                .getDatabase(conf.get("mongo_db"))
//                .getCollection(conf.get("mongo_collection"));
//
//        long docCount = collection.countDocuments();
//        int split = conf.getInt("split", 10);
//        long splitCount = docCount/split;
//
//        List<InputSplit> splitList = new ArrayList<>();
//        for(int i = 0; i < split; i++){
//            splitList.add(new MongoDBInputSplit(i*splitCount, Math.min((i + 1) * splitCount, docCount)));
//        }
//
//        client.close();
//
//        return splitList;
//    }
//
//    @Override
//    public RecordReader<Text, LongWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
//        return new mapreduce.MongoDBRecordReader();
//    }
//
//}