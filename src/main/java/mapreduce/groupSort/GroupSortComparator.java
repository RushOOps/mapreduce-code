package mapreduce.groupSort;

import flowBean.IntPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupSortComparator extends WritableComparator {

    protected GroupSortComparator(){
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((IntPair)a).getFirst().compareTo(((IntPair)b).getFirst());
    }
}
