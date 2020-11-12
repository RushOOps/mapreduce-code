package mapreduce.reduceJoin;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoDBInputFormat extends InputFormat<Text, Text> {

    @Override
    public List<InputSplit> getSplits(JobContext context){
        MongoClient client = new MongoClient("10.66.188.17", 27017);
        MongoCollection<Document> semantic = client.getDatabase("semantic").getCollection("semantic_jointest_01");
        MongoCollection<Document> content = client.getDatabase("semantic").getCollection("semantic_jointest_02");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(new MongoDBInputSplit(0, semantic.countDocuments(), "semantic_jointest_01"));
        inputSplits.add(new MongoDBInputSplit(0, content.countDocuments(), "semantic_jointest_02"));
        client.close();
        return inputSplits;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<Text, Text> reader = new MongoDBRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}
