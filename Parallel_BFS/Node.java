//package org.apache.hadoop.examples;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

// import java.util.HashMap;
// import java.util.Map;
// import java.util.Iterator;
// import java.util.Set;

public class Node {

  public static enum Color {
    WHITE, GRAY, BLACK
  };

  public final int id;// node id = 1,2,3...9
  //private HashMap<Integer, Integer> hashmap = null;// key is edge (node id), value is weight

  private List<Integer> edges = new ArrayList<Integer>();// edge
  private List<Integer> weights = new ArrayList<Integer>();// edge corresponding weight
  private int distance; // cost
  private Color color = Color.WHITE;

  // public HashMap<Integer, Integer> getMap() {
  //   if(hashmap == null) 
  //     hashmap = new HashMap<Integer, Integer>();
  //   return hashmap;
  // }

  public Node(String str) { //input as: 1  2,6|1,10|0|GRAY

    String[] map = str.split("\\s+"); //split by space or tab
    // for(int i =0; i < map.length; i++) {
    //   System.out.println("map_index: ["+ i + "] value: " + "{"+ map[i]+"}");
    // }
    String key = map[0]; // node id, such as 1
    //System.out.println("key: " + key + "\t");
    String value = map[1]; // edge such as  2,6|1,10|0|GRAY
    //System.out.println("value: " + value);
    String[] tokens = value.split("\\|"); // for each input, it contains 4 components: edges, weight, distance and color
    // for(int i =0; i < tokens.length; i++) {
    //   System.out.println("token_index: ["+ i + "] value: " + "{"+ tokens[i]+"}");
    // }
    this.id = Integer.parseInt(key);
    // Store edges
    for (String s : tokens[0].split(",")) { // tokens[0] indicates edges such as 2, 6
      if (s.length() > 0) {
        edges.add(Integer.parseInt(s)); // add edges to ArrayList
      }
    }
    // Store weight
    for(String s : tokens[1].split(",")) {
      if (s.length() > 0) {
        weights.add(Integer.parseInt(s)); // add weights to ArrayList
      }
    }
    // Store distance and color, check whether distance equal infinity, if not get distance from tokens[2]
    if (tokens[2].equals("Integer.MAX_VALUE")) { // tokens[2] indicates distance such as 0 or Integer.MAX_VALUE
      this.distance = Integer.MAX_VALUE;
    } else {
      this.distance = Integer.parseInt(tokens[2]);
    }
    // Store color
    this.color = Color.valueOf(tokens[3]);
    // System.out.println("id: " + id);
    // System.out.println("edges: "  + Arrays.toString(edges.toArray()));
    // System.out.println("weights: " + Arrays.toString(weights.toArray()));
    // System.out.println("distance: " + distance);
    // System.out.println("color: " + color);
  }

  public Node(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public int getDistance() {
    return this.distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }

  public Color getColor() {
    return this.color;
  }

  public void setColor(Color color) {
    this.color = color;
  }

  public List<Integer> getEdges() {
    return this.edges;
  }

  public void setEdges(List<Integer> edges) {
    this.edges = edges;
  }

  public List<Integer> getWeights() {
    return this.weights;
  }

  public void setWeights(List<Integer> weights) {
    this.weights = weights;
  }

  public Text getLine() {
    StringBuffer s = new StringBuffer();
    
    for (int v : edges) {
      s.append(v).append(",");
    }
    s.append("|");

    for (int v : weights) {
      s.append(v).append(",");
    }
    s.append("|");

    if (this.distance < Integer.MAX_VALUE) {
      s.append(this.distance).append("|");
    } else {
      s.append("Integer.MAX_VALUE").append("|");
    }

    s.append(color.toString());

    return new Text(s.toString());
  }

}
