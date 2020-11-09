package mapreduce.userInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 用户数据统计
 * 单次使用
 * 支持增量更新
 */

public class UserInfoDriver {

    private final static String MONGO_IP = "10.66.1.208";
    private final static String MONGO_DB = "semantic";
    private final static String MONGO_TABLE = "semantic_user_info";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_table", MONGO_TABLE);

        // 设置map中间结果压缩
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[1]);

        job.setJarByClass(UserInfoDriver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(UserInfoMapper.class);
//        job.setCombinerClass(WordFrequencyReducer.class);
        job.setReducerClass(UserInfoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }

}
