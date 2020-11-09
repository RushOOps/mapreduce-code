package mapreduce.domainIntentFrequency;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class DomainIntent implements WritableComparable<DomainIntent> {

    private String domain;
    private String intent;

    public DomainIntent(){}

    public DomainIntent(String domain, String intent){
        this.domain = domain;
        this.intent = intent;
    }

    @Override
    public int compareTo(DomainIntent o) {
        return this.domain.compareTo(o.domain);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(domain);
        dataOutput.writeUTF(intent);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.domain = dataInput.readUTF();
        this.intent = dataInput.readUTF();
    }
}
