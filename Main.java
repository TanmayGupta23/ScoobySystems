// 01/15/2022

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
    private final static IntWritable one = new IntWritable(1);
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
        JSONObject spanA = (JSONObject) curSpans.get(j);
        String processIDA = (String) spanA.get("processID");
				JSONObject curProcessA = (JSONObject) processesMap.get(processIDA);
				String processKeyA = (String) curProcessA.get("serviceName");
				String spanA_name = (String) spanA.get("operationName") + processKeyA;
        long t_Astart = (long)spanA.get("startTime");
        long t_Aend  = (long)spanA.get("startTime") + (long)spanA.get("duration");
        for(int k=j+1; k<curSpans.size(); k++) {
          JSONObject spanB = (JSONObject) curSpans.get(k);
          String processIDB = (String) spanB.get("processID");
          JSONObject curProcessB = (JSONObject) processesMap.get(processIDB);
          String processKeyB = (String) curProcessB.get("serviceName");
          String spanB_name = (String) spanB.get("operationName") + processKeyB;
          long t_Bstart = (long)spanB.get("startTime");
          long t_Bend = (long)spanB.get("startTime") + (long)spanB.get("duration");

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
          if (t_Aend < t_Bstart) {
            context.write(new Text(spanA_name+"_s->"+spanB_name+"_s"),one);
            context.write(new Text(spanA_name+"_e->"+spanB_name+"_e"),one);
            context.write(new Text(spanA_name+"_s->"+spanB_name+"_e"),one);
            context.write(new Text(spanA_name+"_e->"+spanB_name+"_s"),one);
          }
          else if (t_Bstart < t_Aend ) {
            context.write(new Text(spanB_name+"_s->"+spanA_name+"_e"),one);
            if (t_Bend < t_Astart) {
              context.write(new Text(spanB_name+"_s->"+spanA_name+"_s"),one);
              context.write(new Text(spanB_name+"_e->"+spanA_name+"_e"),one);
              context.write(new Text(spanB_name+"_e->"+spanA_name+"_s"),one);
            }
            else if (t_Astart < t_Bend) {
              context.write(new Text(spanA_name+"_s->"+spanB_name+"_e"),one);
              if (t_Bstart < t_Astart) context.write(new Text(spanB_name+"_s->"+spanA_name+"_s"),one);
              else if (t_Astart < t_Bstart) context.write(new Text(spanA_name+"_s->"+spanB_name+"_s"),one);
              else context.write(new Text(equality_key(spanA_name,spanB_name,"s","s")),one);

              if (t_Bend < t_Aend) context.write(new Text(spanB_name+"_e->"+spanA_name+"_e"),one);
              else if (t_Aend < t_Bend) context.write(new Text(spanA_name+"_e->"+spanB_name+"_e"),one);
              else context.write(new Text(equality_key(spanA_name,spanB_name,"e","e")),one);
            }
            else {
              context.write(new Text(spanB_name+"_s->"+spanA_name+"_s"),one);
              context.write(new Text(spanB_name+"_e->"+spanA_name+"_e"),one);
              context.write(new Text(spanB_name+"_s->"+spanA_name+"_e"),one);
              context.write(new Text(equality_key(spanA_name,spanB_name,"s","e")),one);
            }
          }
          else {
            context.write(new Text(spanA_name+"_s->"+spanB_name+"_s"),one);
            context.write(new Text(spanA_name+"_e->"+spanB_name+"_e"),one);
            context.write(new Text(spanA_name+"_s->"+spanB_name+"_e"),one);
            context.write(new Text(equality_key(spanA_name,spanB_name,"e","s")),one);
          }
        }
      }
    }
  }

  // Creates string to represent simultaneity relationships to add to the output
  public static String equality_key(String nameA, String nameB, String A_point, String B_point) {
    // Uses alphabetical ordering to avoid A_s=B_s counted separately from B_s=A_s
    if(nameA.compareTo(nameB) < 0) return nameA+"_"+A_point+"="+nameB+"_"+B_point;
    return nameB+"_"+B_point+"="+nameA+"_"+A_point;
  }

  // Defines Reduce processing
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    // Counts total number of occurences of a certain relationship
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
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
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
