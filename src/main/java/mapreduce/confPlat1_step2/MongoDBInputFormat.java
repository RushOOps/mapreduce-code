package mapreduce.confPlat1_step2;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBInputFormat extends InputFormat<Text, Text> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) {
        Configuration conf = jobContext.getConfiguration();

        MongoClient client = new MongoClient(conf.get("mongo_ip"),27017);
        MongoCollection<Document> collection = client.getDatabase(
                conf.get("mongo_db")).getCollection(conf.get("mongo_table_from"));

        // 统计查询后文档的个数
        long count = collection.countDocuments();

        List<InputSplit> splits = new ArrayList<>();
        if(count >= 30){
            int chunks = 30;
            // 每个分片的记录数量
            long chunkSize = count / chunks;

            // 开始分片，只是简单的分配每个分片的数据量
            for (int i = 0; i < chunks; i++) {
                MongoDBInputSplit split;
                if ((i + 1) == chunks) {
                    split = new MongoDBInputSplit(i * chunkSize, count);
                } else {
                    split = new MongoDBInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
                }
                splits.add(split);
            }
            client.close();
        }else{
            splits.add(new MongoDBInputSplit(0, count));
        }
        return splits;

    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new MongoDBRecordReader((MongoDBInputSplit) inputSplit, taskAttemptContext);
    }

}