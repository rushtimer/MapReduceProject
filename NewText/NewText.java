import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Yueyang Wang
 * Date: 02/07/2019
 * Time: 09:38 AM
 * I use the wordPair format, only use word as local variable
 */
public class NewText implements Writable,WritableComparable<NewText> {
  private Text word;
  // constructor 
  public NewText() {
    this.word = new Text();
  }

  public NewText(String word) {
   this.word = new Text(word);
  }

  public NewText(Text word) {
    this.word = word;
  }

  public static NewText read(DataInput in) throws IOException {
    NewText newText = new NewText();
    newText.readFields(in);
    return newText;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write a single word
    word.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read a single word
    word.readFields(in);
  }

  @Override
  public int compareTo(NewText other) {
    // reversed alphabetical order
    return other.getWord().compareTo(this.word);
  }
/*  public int CompareToValue(Object o) {
   NewText other = (NewText) o;
   return this.value - other.value;
  }*/
/*  @Override
  public int hashCode() {
    int result = (word != null) ? word.hashCode() : 0;
    result = result % 3; 
    return result;
  }*/

  public Text getWord() {
    return word;
  }

  public void setWord(String word) {
    this.word.set(word);
  }

  @Override
  public String toString() {
    return ""+word+"";
  }

/*  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NewText newText = (NewText) o;

    if (value != 0 ? !value == (newText.value) : newText.value != 0) return false;
    if (word != null ? !word.equals(newText.word) : newText.word != null) return false;

    return true;    
  }*/

}