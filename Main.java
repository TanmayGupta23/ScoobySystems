// 1/31/22
// Anshul Rastogi and Tanmay Gupta

package hdppkg;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

// Imports for JSON parsing
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

// Imports for Hadoop
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Main {

  // Defines Mapper processing
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable success = new IntWritable(1); // e.g. Event X_s->Y_s, where X alphabetically precedes Y
    private final static IntWritable reverse = new IntWritable(-1); // e.g Y_s->X_s
    private final static IntWritable simult = new IntWritable(0); // e.g. Y_s at same time as X_s
    
    private Text relationship = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Reads the JSON file
      String jsonString = value.toString();

      //Parses JSON
      JSONObject jsonObject = (JSONObject) JSONValue.parse(jsonString);
			JSONArray traces = (JSONArray) jsonObject.get("data");
      JSONObject curTrace = (JSONObject) traces.get(0);
      JSONArray curSpans = (JSONArray) curTrace.get("spans");
      JSONObject processesMap = (JSONObject) curTrace.get("processes");

      // Loops through pairs of spans A, B in a trace
      for(int j=0; j<curSpans.size()-1; j++) {
        JSONObject span1 = (JSONObject) curSpans.get(j);
        String processID1 = (String) span1.get("processID");
        JSONObject curProcess1 = (JSONObject) processesMap.get(processID1);
        String processKey1 = (String) curProcess1.get("serviceName");
        String span1_name = (String) span1.get("operationName") + processKey1;
        for(int k=j+1; k<curSpans.size(); k++) {
          JSONObject span2 = (JSONObject) curSpans.get(k);
          String processID2 = (String) span2.get("processID");
          JSONObject curProcess2 = (JSONObject) processesMap.get(processID2);
          String processKey2 = (String) curProcess2.get("serviceName");
          String span2_name = (String) span2.get("operationName") + processKey2;
          
          // To ensure that all relationships are interpreted the same regardless of the order they appear in within each individual trace, we define the relationship name by alphabetical order of the span names
          String pairName;
          long t_Astart, t_Aend, t_Bstart, t_Bend;
          if(span2_name.compareTo(span1_name) < 0) { 
            pairName = span2_name+"|"+span1_name+"|";
            t_Astart = (long)span2.get("startTime");
            t_Aend  = (long)span2.get("startTime") + (long)span2.get("duration");
            t_Bstart = (long)span1.get("startTime");
            t_Bend = (long)span1.get("startTime") + (long)span1.get("duration");
          }
          else{
            pairName = span1_name+"|"+span2_name+"|";
            t_Astart = (long)span1.get("startTime");
            t_Aend  = (long)span1.get("startTime") + (long)span1.get("duration");
            t_Bstart = (long)span2.get("startTime");
            t_Bend = (long)span2.get("startTime") + (long)span2.get("duration");
          }


          // Automatically known relationships:
          // A_End -> A_Start - Always violated
          // A_Start -> A_Start - Always violated
          // A_End -> A_End - Always violated
          // B_End -> B_Start - Always violated
          // B_Start -> B_Start - Always violated
          // B_End -> B_End - Always violated
          // A_Start -> A_End - Always true
          // B_Start -> B_End - Always true

          // This network of conditional statements adds relationships to the output by going through potential cases
          // We give different relationships between the same two events the same key so that they all go to the same reducer, and we can apply the threshold there
          if (t_Aend < t_Bstart) {
            context.write(new Text(pairName+"ss"),success); // Note that "success" means A happens-before B, and "se" means that the relationship is between events A_s & B_e
            context.write(new Text(pairName+"ee"),success);
            context.write(new Text(pairName+"se"),success);
            context.write(new Text(pairName+"es"),success);
          }
          else if (t_Bstart < t_Aend ) {
            context.write(new Text(pairName+"es"),reverse);
            if (t_Bend < t_Astart) {
              context.write(new Text(pairName+"ss"),reverse);
              context.write(new Text(pairName+"ee"),reverse);
              context.write(new Text(pairName+"se"),reverse);
            }
            else if (t_Astart < t_Bend) {
              context.write(new Text(pairName+"se"),success);
              if (t_Bstart < t_Astart) context.write(new Text(pairName+"ss"),reverse);
              else if (t_Astart < t_Bstart) context.write(new Text(pairName+"ss"),success);
              else context.write(new Text(pairName+"ss"),simult);

              if (t_Bend < t_Aend) context.write(new Text(pairName+"ee"),reverse);
              else if (t_Aend < t_Bend) context.write(new Text(pairName+"ee"),success);
              else context.write(new Text(pairName+"ee"),simult);
            }
            else {
              context.write(new Text(pairName+"ss"),reverse);
              context.write(new Text(pairName+"ee"),reverse);
              context.write(new Text(pairName+"es"),reverse);
              context.write(new Text(pairName+"se"),simult);
            }
          }
          else {
            context.write(new Text(pairName+"ss"),success);
            context.write(new Text(pairName+"ee"),success);
            context.write(new Text(pairName+"se"),success);
            context.write(new Text(pairName+"es"),simult);
          }
        }
      }
    }
  }


  // Defines Reduce processing
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable(1);

    // Counts total number of occurences of a certain relationship
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      long numTraces=context.getConfiguration().getLong("mapred.map.tasks", 1000);
      long successes = 0;
      long reverses = 0;
      long simults = 0;
      for (IntWritable val: values) {
        if(val.get()==-1) reverses++;
        else if (val.get()==1) successes++;
        else simults++;
      }
      long violations = reverses+simults;
      long unknowns = numTraces-successes-violations;
      String strKey = key.toString();
      int id1=strKey.indexOf("|");
      String A = strKey.substring(0,id1);
      int id2 = strKey.indexOf("|",id1+1);
      String B = strKey.substring(id1+1, id2);
      String Ase = strKey.substring(id2+1, id2+2);
      String Bse = strKey.substring(id2+2, id2+3);
      if ((double)successes/numTraces>0.95) { // USER can modify this to have a different threshold condition
        context.write(new Text(A+Ase+"->"+B+Bse), result);
      }
      else if ((double)reverses/numTraces>0.95) { // USER can modify this to have a different threshold condition
        context.write(new Text(B+Bse+"->"+A+Ase), result);
      }
    }
  }

  // Configures Hadoop settings
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    // Sets delimiter to something unlikely to occur in any of our trace files
    // so that files remain intact as they are sent to mappers
    conf.set("textinputformat.record.delimiter", "!!!!!!");
    
    Job job = Job.getInstance(conf, "word count adapted");
    job.setJarByClass(Main.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class); // TODO: Maybe create new better class for this later (positive number = success no matter number, negative = reverses, and it is fine not to combine simults because rare anyway)
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}




