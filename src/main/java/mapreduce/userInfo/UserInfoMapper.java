package mapreduce.userInfo;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.StringUtil;

import java.io.IOException;

public class UserInfoMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(StringUtil.isEmpty(value.toString())) return;
        JSONObject valueJson = JSONObject.parseObject(value.toString());

        String mac = valueJson.getString("query_mac");
        String ip = valueJson.getString("query_ip");
        if (!StringUtil.isEmpty(ip)) ip = ip.replace("\t", "");
        if(StringUtil.isEmpty(mac)) return;
        JSONObject query = valueJson.getJSONObject("query");
        if(query == null) return;
        JSONObject mapOut = new JSONObject();
        mapOut.put("ip", ip);
        mapOut.put("user_id", query.getString("user_id"));
        mapOut.put("device_id", query.getString("deviceid"));
        mapOut.put("client_type", query.getString("clienttype"));
        mapOut.put("provider_code", query.getString("providerCode"));
        mapOut.put("time", valueJson.getString("time"));
        mapOut.put("sversion", valueJson.getString("sversion"));
        mapOut.put("latitude", query.getString("latitude"));
        mapOut.put("longitude", query.getString("longitude"));
        mapOut.put("city_location", query.getString("cityLocation"));

        context.write(new Text(mac), new Text(mapOut.toString()));
    }

}
