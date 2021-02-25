package mapreduce.topNMix;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNMixBean implements WritableComparable<TopNMixBean> {
    private String course;
    private Double avgScore;

    public TopNMixBean() {
    }

    public TopNMixBean(String course, Double avgScore) {
        this.course = course;
        this.avgScore = avgScore;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public Double getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(Double avgScore) {
        this.avgScore = avgScore;
    }

    @Override
    public int compareTo(TopNMixBean o) {
        int cmp = this.course.compareTo(o.course);
        if(cmp == 0){
            return o.avgScore.compareTo(this.avgScore);
        }
        return cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeDouble(avgScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.course = in.readUTF();
        this.avgScore = in.readDouble();
    }
}
