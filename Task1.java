import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.ArrayList;


public class Task1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();
        String str = value.toString();
        String[] result = str.toString().split(";");
        int n = result.length;

        String year = result[n-2];
		String titletype = result[n-4];

        if (!year.equals("\\N") && titletype.equals("movie")) {
            if (Integer.parseInt(year) >= 2000 && Integer.parseInt(year) <= 2006 ) {
                String genreList = result[n-1];
                String genreArray[] = genreList.split(",");
				ArrayList<String> gl = new ArrayList<String>(genreArray.length);
				for(String genre: genreArray){
					gl.add(genre);
				}
				if(gl.contains("Comedy") && gl.contains("Romance")){
					String intermediateKey = "[2000-2006]" + " " + "Comedy;Romance";
                        word.set(intermediateKey);
                        context.write(word, one);
					
				}
				if(gl.contains("Action") && gl.contains("Drama")){
					String intermediateKey = "[2000-2006]" + " " + "Action;Drama";
                        word.set(intermediateKey);
                        context.write(word, one);
					
						
				}
				if(gl.contains("Adventure") && gl.contains("Sci-Fi")){
					String intermediateKey = "[2000-2006]" + " " + "Adventure;Sci-Fi";
                        word.set(intermediateKey);
                        context.write(word, one);
					
				}
				
				
            }

        }
		
		if (!year.equals("\\N") && titletype.equals("movie")) {
            if (Integer.parseInt(year) >= 2007 && Integer.parseInt(year) <= 2013 ) {
                String genreList = result[n-1];
                String genreArray[] = genreList.split(",");
				ArrayList<String> gl = new ArrayList<String>(genreArray.length);
				for(String genre: genreArray){
					gl.add(genre);
				}
				if(gl.contains("Comedy") && gl.contains("Romance")){
					String intermediateKey = "[2007-2013]" + " " + "Comedy;Romance";
                        word.set(intermediateKey);
                        context.write(word, one);
					
				}
				if(gl.contains("Action") && gl.contains("Drama")){
					String intermediateKey = "[2007-2013]" + " " + "Action;Drama";
                        word.set(intermediateKey);
                        context.write(word, one);
					
						
				}
				if(gl.contains("Adventure") && gl.contains("Sci-Fi")){
					String intermediateKey = "[2007-2013]" + " " + "Adventure;Sci-Fi";
                        word.set(intermediateKey);
                        context.write(word, one);
					
				}
				
				
            }

        }
		
		if (!year.equals("\\N") && titletype.equals("movie")) {
            if (Integer.parseInt(year) >= 2014 && Integer.parseInt(year) <= 2020 ) {
                String genreList = result[n-1];
                String genreArray[] = genreList.split(",");
				ArrayList<String> gl = new ArrayList<String>(genreArray.length);
				for(String genre: genreArray){
					gl.add(genre);
				}
				if(gl.contains("Comedy") && gl.contains("Romance")){
					String intermediateKey = "[2014-2020]" + " " + "Comedy;Romance";
                        word.set(intermediateKey);
                        context.write(word, one);
					
				}
				if(gl.contains("Action") && gl.contains("Drama")){
					String intermediateKey = "[2014-2020]" + " " + "Action;Drama";
                        word.set(intermediateKey);
                        context.write(word, one);
					
						
				}
				if(gl.contains("Adventure") && gl.contains("Sci-Fi")){
					String intermediateKey = "[2014-2020]" + " " + "Adventure;Sci-Fi";
                        word.set(intermediateKey);
                        context.write(word, one);
					
				}
				
				
            }

        }
		
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);

      String strArr[] = key.toString().split(" ");
      if (strArr.length > 1) {
          String year = strArr[0];
          String genre = strArr[1];
    
          String newKey = year + "," + genre + ",";
          key.set(newKey);
      }
      context.write(key, result);
	  
	  
    }
  }

  public static void main(String[] args) throws Exception {
    Path out = new Path(args[1]);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Task1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    if (fs.exists(out))
        fs.delete(out, true);
    FileOutputFormat.setOutputPath(job, out);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}