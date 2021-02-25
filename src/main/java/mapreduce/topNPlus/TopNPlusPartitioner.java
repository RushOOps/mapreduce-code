package mapreduce.topNPlus;

import org.apache.hadoop.mapreduce.Partitioner;


public class TopNPlusPartitioner extends Partitioner<TopNPlusBean, Integer>{
    @Override
    public int getPartition(TopNPlusBean topNPlusBean, Integer integer, int numPartitions) {
        return (topNPlusBean.getCourse().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
