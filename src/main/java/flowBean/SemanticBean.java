package flowBean;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class SemanticBean implements Writable {

    private String domain;
    private String intent;
    private JSONObject semantic;

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public String toString() {
        return "SemanticBean{" +
                "domain='" + domain + '\'' +
                ", intent='" + intent + '\'' +
                ", semantic=" + semantic.toString() +
                '}';
    }
}
