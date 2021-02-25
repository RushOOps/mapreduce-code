package mapreduce.topNMix;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNMixComparator extends WritableComparator {

    public TopNMixComparator() {
        super(TopNMixBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((TopNMixBean)a).getCourse().compareTo(((TopNMixBean)b).getCourse());
    }
}
