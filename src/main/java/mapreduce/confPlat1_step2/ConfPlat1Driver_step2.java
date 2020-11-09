package mapreduce.confPlat1_step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 统计同一个文本对应的mac地址（10个以上），5天的，每天更新并且删除最早一天，数据聚合
 * 每日任务
 * 不支持增量更新
 */

public class ConfPlat1Driver_step2 extends Configured implements Tool {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic";
    private final static String MONGO_TABLE_FROM = "conf_plat_query_mac_unprocessed";
    private final static String MONGO_TABLE_TO = "conf_plat_query_mac_processed";

    @Override
    public int run(String[] args) throws Exception {

//        BasicConfigurator.configure();

        Configuration conf = getConf();
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_table_from", MONGO_TABLE_FROM);
        conf.set("mongo_table_to", MONGO_TABLE_TO);

        // 设置map中间结果压缩
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[0]);

        job.setJarByClass(ConfPlat1Driver_step2.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ConfPlat1Mapper_step2.class);
        job.setReducerClass(ConfPlat1Reducer_step2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(MongoDBInputFormat.class);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new ConfPlat1Driver_step2(), args));

    }
}
