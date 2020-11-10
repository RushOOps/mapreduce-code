package mapreduce.mergeFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileReader extends RecordReader<Text, BytesWritable> {

    Text key = new Text();
    BytesWritable value = new BytesWritable();
    FileSplit split;
    Configuration conf;
    boolean hasNext = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if(hasNext){
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
            try(FSDataInputStream fis = fs.open(path)){
                byte[] buf = new byte[(int) split.getLength()];
                IOUtils.readFully(fis, buf, 0, buf.length);
                key.set(path.toString());
                value.set(buf, 0, buf.length);
            }
            hasNext = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() {

    }

}
