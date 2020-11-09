package mapreduce.musicUnmatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 测试
 * chat数据根据字数统计
 * 单次使用
 * 支持增量更新
 */

public class MusicUnmatchDriver extends Configured implements Tool {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic_data_raw";
    private final static String MONGO_TABLE = "music_unmatch";
    private final static String MONGO_TABLE_HISTORY = "music_unmatch_history";
    private final static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(conf, new MusicUnmatchDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_table", MONGO_TABLE);
        conf.set("mongo_table_history", MONGO_TABLE_HISTORY);

        // 设置map中间结果压缩
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[1]);

        job.setJarByClass(MusicUnmatchDriver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MusicUnmatchMapper.class);
//        job.setCombinerClass(MusicUnmatchReducer.class);
        job.setReducerClass(MusicUnmatchReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        return job.waitForCompletion(true)? 0 : 1;
    }
}
