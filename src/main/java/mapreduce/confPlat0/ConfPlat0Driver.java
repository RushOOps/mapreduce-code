package mapreduce.confPlat0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 统计每天请求用户的mac地址的频次，管理系统从中取前100的mac显示
 * 每日任务
 * 不支持增量更新
 */

public class ConfPlat0Driver {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic";

    /**
     * @param args
     * args[0] 统计文件目录
     * args[1] 输出数据库
     * args[2] job名称
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_table", args[1]);

        // 设置map中间结果压缩
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[2]);

        job.setJarByClass(ConfPlat0Driver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ConfPlat0Mapper.class);
//        job.setCombinerClass(WordFrequencyReducer.class);
        job.setReducerClass(ConfPlat0Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }

}
