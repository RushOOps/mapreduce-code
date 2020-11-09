package mapreduce.semanticMerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 知识图谱
 * 热门实体统计数据聚合
 * 每日任务
 * 不支持增量更新
 */

public class SemanticMergeDriver extends Configured implements Tool {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic";
    private final static String MONGO_TABLE_FROM = "semantic_online_domain_201909~202008";
    private final static String MONGO_TABLE_TO = "semantic_content_201909~202008";

    private final static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(conf, new SemanticMergeDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_table_from", MONGO_TABLE_FROM);
        conf.set("mongo_table_to", MONGO_TABLE_TO);
        conf.set("split_num", args[1]);

        // 设置map中间结果压缩
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[0]);

        job.setJarByClass(SemanticMergeDriver.class);

        job.setMapperClass(SemanticMergerMapper.class);
//        job.setCombinerClass(WordFrequencyReducer.class);
        job.setReducerClass(SemanticMergerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(MongoDBInputFormat.class);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        return job.waitForCompletion(true)? 0 : 1;
    }
}
