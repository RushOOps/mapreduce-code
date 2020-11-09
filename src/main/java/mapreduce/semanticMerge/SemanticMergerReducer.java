package mapreduce.semanticMerge;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SemanticMergerReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> semantic = new HashSet<>();
        for(Text t : values){
            semantic.add(t.toString());
        }
        if(semantic.size() == 1){
            context.write(new Text("OBJECT"), new Text(key.toString()+"@@@"+semantic.iterator().next()));
        }else{
            JSONArray array = new JSONArray();
            for(String singleSemantic : semantic){
                if(array.size() >= 3) break;
                array.add(JSONObject.parseObject(singleSemantic));
            }
            context.write(new Text("ARRAY"), new Text(key.toString()+"@@@"+array.toString()));
        }
    }

}



