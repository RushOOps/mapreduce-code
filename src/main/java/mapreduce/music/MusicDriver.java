package mapreduce.music;

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
 * 音乐领域semantic键值对统计
 * 单次执行
 * 不支持增量更新
 */

public class MusicDriver {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic_data_raw";
    private final static String MONGO_TABLE = "music_tencent_inc";

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
        // 设置作业名称
        job.setJobName(args[1]);

        job.setJarByClass(MusicDriver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MusicMapper.class);
//        job.setCombinerClass(WordFrequencyReducer.class);
        job.setReducerClass(MusicReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }

}
