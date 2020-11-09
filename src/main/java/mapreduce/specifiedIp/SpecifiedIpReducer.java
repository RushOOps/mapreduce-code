package mapreduce.specifiedIp;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SpecifiedIpReducer extends Reducer<Text, Text, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if(key.toString().equals("mac")){
            Set<String> macSet = new HashSet<>();
            for(Text t : values){
                macSet.add(t.toString());
            }
            macSet.remove("null");
            context.getCounter("OUTPUT", "mac").increment(macSet.size());
        }else{
            context.getCounter("OUTPUT", key.toString()).increment(Iterables.size(values));
        }

    }

}
