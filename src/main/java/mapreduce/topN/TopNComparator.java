package mapreduce.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNComparator extends WritableComparator {

    public TopNComparator() {
        super(TopNBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((TopNBean)a).getCourse().compareTo(((TopNBean)b).getCourse());
    }
}
