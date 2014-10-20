package org.myorg;
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class MatrixMult{

  // 1st phase mapper
  // 1) read in <bytes, a line of file>
  // 2) group matrix A column i with matrix B row i
  // 3) sends out <i, <r,c,value,matrix_name>>
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(false){
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("A")) {
                for (int k = 0; k < p; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k);
                    outputValue.set("A," + indicesAndValue[2] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]);
                    outputValue.set("B," + indicesAndValue[1] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }

        if(false){
          Text outputKey = new Text();
          outputKey.set(key.toString());
          context.write(outputKey,value);
        }

        if(true){
          // get matrix C (m by p) size
          Configuration conf = context.getConfiguration();
          int m = Integer.parseInt(conf.get("m"));
          int p = Integer.parseInt(conf.get("p"));

          // setup a line of file
          String line = value.toString();
          String[] indicesAndValue = line.split(",");

          // text format
          int rIdx = 0; // row
          int cIdx = 1; // col
          int vIdx = 2; // value
          int mIdx = 3; // matrix name

          // allocate output variables
          Text outputKey = new Text();
          Text outputValue = new Text();

          // matrix A do this
          if (indicesAndValue[mIdx].equals("A")){
            outputKey.set(indicesAndValue[cIdx]); // matrix A column num
            outputValue.set(value.toString());
            context.write(outputKey,outputValue);
          }
          // matrix B do this
          else { 
            outputKey.set(indicesAndValue[rIdx]); // matrix B column num
            outputValue.set(value.toString());
            context.write(outputKey,outputValue);
          }
        }
      }
    }

    // 2nd phase:
    // input key: The matching number col of A and row of B
    // input value: list of <row,col,value,matrix name>
    // output key: <row,col> of matrix C
    // output value: all the products
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(false){
            String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("A")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float a_ij;
            float b_jk;
            for (int j = 0; j < n; j++) {
                a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += a_ij * b_jk;
            }
            if (result != 0.0f) {
                context.write(null, new Text(key.toString() + "," + Float.toString(result)));
            }
        } //if

        if(false){
          Text outputValue = new Text();
          //outputValue = values.iterator().next();
          int i=0;
          while(values.iterator().hasNext()){
            context.write(key,new Text(values.iterator().next()+",combiner"+Integer.toString(i)));
            i++;
          }
        }
        
        if(true){
          // text format
          int rIdx = 0; // row
          int cIdx = 1; // col
          int vIdx = 2; // value
          int mIdx = 3; // matrix name

          // get the output key ready
          // it is row of A, col of B
          // each of the element of A need to multiply all the B elements
          Iterator<Text> iter_i = values.iterator();
          while(iter_i.hasNext()){
            // setup a line of file
            String line_i = iter_i.next().toString();
            String[] indicesAndValue_i = line_i.split(",");
            // find matrix A
            if(indicesAndValue_i[mIdx].equals("A")){
              if(false){
                // setup outputKey and outputValue
                Text outputKey = new Text();
                Text outputValue = new Text();
                outputKey.set(indicesAndValue_i[rIdx]); // row of A, col of B
                context.write(outputKey,new Text("i am A"));
                continue;
              }
              Iterable<Text> values2 = values;
              Iterator<Text> iter_j = values2.iterator();
              while(iter_j.hasNext()){
                // setup a line of file
                String line_j = iter_j.next().toString();
                String[] indicesAndValue_j = line_j.split(",");
                if(true){
                  // setup outputKey and outputValue
                  Text outputKey = new Text();
                  Text outputValue = new Text();
                  //outputKey.set(); // row of A, col of B
                  context.write(new Text(line_i),new Text(line_j));
                  continue;
                }
                // multiply each matrix B
                if(indicesAndValue_j[mIdx].equals("B")){
                  if(false){
                    // setup outputKey and outputValue
                    Text outputKey = new Text();
                    Text outputValue = new Text();
                    outputKey.set(indicesAndValue_i[rIdx] + "," + indicesAndValue_j[cIdx]); // row of A, col of B
                    context.write(outputKey,new Text("i am B"));
                    continue;
                  }
                  // setup outputKey and outputValue
                  Text outputKey = new Text();
                  Text outputValue = new Text();
                  float valA = Float.parseFloat(indicesAndValue_i[vIdx]);
                  float valB = Float.parseFloat(indicesAndValue_j[vIdx]);
                  float result = valA * valB;
                  outputKey.set(indicesAndValue_i[rIdx] + "," + indicesAndValue_j[cIdx]); // row of A, col of B
                  outputValue.set(Float.toString(result));
                  context.write(outputKey,outputValue);
                }
              }
            }
          }
        }
      }
    }

    // 3rd phase:
    // output key: <row,col> of matrix C
    // output value: summation of all the related products
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          if(false){
            String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("A")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float a_ij;
            float b_jk;
            for (int j = 0; j < n; j++) {
                a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += a_ij * b_jk;
            }
            if (result != 0.0f) {
                context.write(null, new Text(key.toString() + "," + Float.toString(result)));
            }
        } //if
        //Text outputValue = new Text();
        //outputValue = values.iterator().next();
        int i=0;
        while(values.iterator().hasNext()){
          context.write(key,new Text(values.iterator().next()+",reducer"+Integer.toString(i)));
          i++;
        }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
        conf.set("m", "2");
        conf.set("n", "5");
        conf.set("p", "3");
 
        Job job = new Job(conf, "MatrixMult");
        job.setJarByClass(MatrixMult.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}
