import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class wordpair implements Writable,WritableComparable<wordpair> {

    private Text word;
    private Text docID;

    public wordpair(Text word, Text docID) {
        this.word = word;
        this.docID = docID;
    }

    public wordpair(String word, String docID) {
        this(new Text(word),new Text(docID));
    }

    public wordpair() {
        this.word = new Text();
        this.docID = new Text();
    }
    // alphabetic order
    @Override
    public int compareTo(wordpair other) {                         // A compareTo B
        int returnVal = this.word.compareTo(other.getWord());      // return -1: A < B
        if(returnVal != 0){                                        // return 0: A = B
            return returnVal;                                      // return 1: A > B
        }
        if(this.docID.toString().equals("*")){
            return -1;
        }else if(other.getdocID().toString().equals("*")){
            return 1;
        }
        return this.docID.compareTo(other.getdocID());
    }

    public static wordpair read(DataInput in) throws IOException {
        wordpair wordPair = new wordpair();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        docID.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        docID.readFields(in);
    }

    @Override
    public String toString() {
        return word + " " + docID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        wordpair wordPair = (wordpair) o;

        if (docID != null ? !docID.equals(wordPair.docID) : wordPair.docID != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (word != null) ? word.hashCode() : 0;
        //result = word.hashCode() % 3;
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setdocID(String docID){
        this.docID.set(docID);
    }

    public Text getWord() {
        return word;
    }

    public Text getdocID() {
        return docID;
    }

}