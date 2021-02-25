package mapreduce.TopNPlus;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNPlusComparator extends WritableComparator {

    public TopNPlusComparator() {
        super(TopNPlusBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((TopNPlusBean)a).getCourse().compareTo(((TopNPlusBean)b).getCourse());
    }
}
