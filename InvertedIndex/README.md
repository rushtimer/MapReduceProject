# Inverted Index
Generate the inverted index given a set of input files.

The input contains a set of text files. Each file contains words without punctuation marks in multiple lines.

The generated inverted index is distributively saved in multiple files. An output file contains multiple lines. Each line consists of a term (i.e., a word) and the posting list.

In each output file, those lines are listed in an alphabetic order.

Each posting list is in such a format as “file name:# of occurrence;file name:# of occurrence...”. The posting list needs to be in the order of file names. Example: “file0:18;file1:20;file2:3”.

Specify 3 reducers.

Use pairs approach to generate complex keys, i.e., (term, filename), in the mapper stage. The value is the total number of occurrences the term appears in the file.

Apply in-mapper combining to figure out the total number of occurrences a term appears in a file.

