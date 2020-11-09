package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class HdfsCreate {
    public static void main(String[] args) throws IOException {
        String from = "/Users/crash/Documents/test";
        String to = "hdfs://localhost/user/crash/test";
        Configuration conf = new Configuration();
        InputStream in = new BufferedInputStream(new FileInputStream(from));
        FileSystem fs = FileSystem.get(URI.create(to), conf);
        // 这里lambda表达式是progress函数，每读一次都会调用一次这个函数
        OutputStream out = fs.create(new Path(to), ()->System.out.print("."));
        IOUtils.copyBytes(in, out, 4096, false);
        in.close();
        out.close();
    }
}
