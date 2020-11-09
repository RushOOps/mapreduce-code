package mapreduce.specifiedIp;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SpecifiedIpMapper extends Mapper<Object, Text, Text, Text> {

    private final List<String> IP_LIST = new ArrayList<String>(){
        {
            add("114.55.209.183");
            add("47.99.246.139");
            add("121.40.160.190");
            add("118.31.125.243");
            add("121.36.100.245");
            add("121.36.41.129");
            add("121.36.10.99");
            add("121.36.96.137");
        }
    };

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String queryIp = valueJson.getString("query_ip");
        String mac = valueJson.getString("query_mac");

        if(StringUtil.isEmpty(queryIp)){
            context.getCounter("Invalid data", "Ip Empty").increment(1);
            return;
        }
        if(!IP_LIST.contains(queryIp)) return;

        if(StringUtil.isEmpty(mac)){
            context.getCounter("Invalid data", "Mac Empty").increment(1);
            mac = "null";
        }

        context.write(new Text(queryIp), new Text());
        context.write(new Text("mac"), new Text(mac));
    }

}
