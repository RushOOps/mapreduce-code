package mapreduce.semanticCal;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import flowBean.MongoDBInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBInputFormat extends InputFormat<Text, IntWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) {

        Configuration conf = context.getConfiguration();
        MongoClient client =  new MongoClient(conf.get("mongo_ip"), 27017);
        MongoCollection<Document> collection = client
                .getDatabase(conf.get("mongo_db"))
                .getCollection(conf.get("mongo_collection"));

        long total = collection.countDocuments();
        int splitNum = conf.getInt("split_num", 10);
        long docSizeInSplit = total%splitNum==0 ? total/splitNum : (total/splitNum)+1;

        List<InputSplit> splits = new ArrayList<>();
        for(int i = 0; i < splitNum-1; i++){
            splits.add(new MongoDBInputSplit(i * docSizeInSplit, (i+1)*docSizeInSplit));
        }
        splits.add(new MongoDBInputSplit((splitNum-1) * docSizeInSplit, total));

        client.close();
        return splits;
    }

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new MongoDBRecordReader();
    }

}