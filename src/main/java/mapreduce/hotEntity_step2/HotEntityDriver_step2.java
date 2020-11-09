package mapreduce.hotEntity_step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 知识图谱
 * 热门实体统计数据聚合
 * 每日任务
 * 不支持增量更新
 */

public class HotEntityDriver_step2 {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB_FROM = "semantic";
    private final static String MONGO_DB_TO = "semantic_data_processed";
    private final static String MONGO_TABLE_FROM = "semantic_hot_entity_temp";
    private final static String MONGO_TABLE_TO = "hot_entity";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db_from", MONGO_DB_FROM);
        conf.set("mongo_db_to", MONGO_DB_TO);
        conf.set("mongo_table_from", MONGO_TABLE_FROM);
        conf.set("mongo_table_to", MONGO_TABLE_TO);

        // 设置map中间结果压缩
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[0]);

        job.setJarByClass(HotEntityDriver_step2.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(HotEntityMapper_step2.class);
//        job.setCombinerClass(WordFrequencyReducer.class);
        job.setReducerClass(HotEntityReducer_step2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(MongoDBInputFormat.class);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }

}
