package mapreduce.specifiedIp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 统计ip的数据
 * 单次使用
 * 支持增量更新
 */
public class SpecifiedIpDriver extends Configured implements Tool {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic";
    private final static String MONGO_TABLE = "semantic_sepecifiedIp";

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new SpecifiedIpDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
//        BasicConfigurator.configure();

        Configuration conf = getConf();
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_table", MONGO_TABLE);

        // 设置map中间结果压缩
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[1]);

        job.setJarByClass(SpecifiedIpDriver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(SpecifiedIpMapper.class);
        job.setReducerClass(SpecifiedIpReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
//        job.setOutputFormatClass(MongoDBOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/output"));


        return job.waitForCompletion(true)?0:1;
    }
}
