package mapreduce.topNMix;

import org.apache.hadoop.mapreduce.Partitioner;


public class TopNMixPartitioner extends Partitioner<TopNMixBean, Integer>{
    @Override
    public int getPartition(TopNMixBean topNMixBean, Integer integer, int numPartitions) {
        return (topNMixBean.getCourse().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
