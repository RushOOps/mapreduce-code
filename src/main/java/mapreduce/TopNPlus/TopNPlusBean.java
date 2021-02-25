package mapreduce.TopNPlus;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNPlusBean implements WritableComparable<TopNPlusBean> {
    private String course;
    private Integer avgScore;

    public TopNPlusBean() {
    }

    public TopNPlusBean(String course, Integer avgScore) {
        this.course = course;
        this.avgScore = avgScore;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public Integer getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(Integer avgScore) {
        this.avgScore = avgScore;
    }

    @Override
    public int compareTo(TopNPlusBean o) {
        int cmp = this.course.compareTo(o.course);
        if(cmp == 0){
            return o.avgScore.compareTo(this.avgScore);
        }
        return cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeInt(avgScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.course = in.readUTF();
        this.avgScore = in.readInt();
    }
}
