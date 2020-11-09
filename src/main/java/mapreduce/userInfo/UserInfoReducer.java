package mapreduce.userInfo;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class UserInfoReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String firstTime = "9999-99-99 99:99:99";
        String lastTime = "0000-00-00 00:00:00";
        long count = 0;
        Text text = new Text();
        for (Text value : values) {
            count++;
            text = value;
            JSONObject valueJson = JSONObject.parseObject(text.toString());
            String time = valueJson.getString("time");
            if (time.equals("null")) continue;
            if (time.compareTo(lastTime) > 0) {
                lastTime = time;
            }
            if (firstTime.compareTo(time) > 0) {
                firstTime = time;
            }
        }
        JSONObject mapOut = JSONObject.parseObject(text.toString());
        mapOut.remove("time");
        JSONObject result = new JSONObject();
        result.putAll(mapOut);
        result.put("last_time", lastTime);
        result.put("first_time", firstTime);
        result.put("count", count);

        context.write(key, new Text(result.toString()));
    }

}
