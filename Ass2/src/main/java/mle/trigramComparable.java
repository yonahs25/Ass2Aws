package mle;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class trigramComparable implements WritableComparable<trigramComparable> {
    private String[] triagram;
    private double probs;
    private String ngram;

    public trigramComparable(){}

    @Override
    public String toString(){ // return the ngram and the probability separated by a tab
        return this.ngram + "\t" + this.probs;
    }

    public trigramComparable(Text ngram){
        this.ngram = ngram.toString();
        String[] parts = this.ngram.split("\t");
        this.triagram = parts[0].split(" ");
        this.probs = Double.parseDouble(parts[1]);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.toString()); // write the ngram
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String value = in.readUTF(); // read the ngram and the probability
        String[] parts = value.split("\t");
        this.triagram = parts[0].split(" ");
        this.probs = Double.parseDouble(parts[1]);
        this.ngram = parts[0]; // check this

        ///Maybe this is needed:
        // if (!(parts.length > 1)){
        //     this.ngram = "";
        //     this.probs = Double.parseDouble(parts[0]);
        //     this.triagram = new String[0];
        // }
    }

    @Override
    public int compareTo(trigramComparable other) {
        // comparing first by w1w2 ascending, then by probability descending
        //Need to check what about the length, how sometimes it can be different
        if (this.triagram[0].compareTo(other.triagram[0]) != 0) return this.triagram[0].compareTo(other.triagram[0]);
        if (this.triagram[1].compareTo(other.triagram[1]) != 0) return this.triagram[1].compareTo(other.triagram[1]);
        if (this.probs > other.probs) return -1;
        if (this.probs < other.probs) return 1;
        return 0;
    }


}
