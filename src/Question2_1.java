import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class Question2_1 {

	final static public int K_DEFAULT = 5;

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//System.out.println("map go");
			double latitude = 0, longitude = 0;
			String uTags = null, mTags = null;
			String[] fields = value.toString().split("\\t");

			// Discard invalid inputs
			if (fields.length < 11) {
				return;
			}

			for (int i = 0; i < fields.length; i++) {
				String field = fields[i];

				switch (i) {

					// User tags
					case 8:
						//uTags = field.split(",");
						uTags = java.net.URLDecoder.decode(field, "UTF-8");
						//System.out.println("uTags = " + uTags);
						break;

					// Machine tags
					case 9:
						mTags = java.net.URLDecoder.decode(field, "UTF-8");
						break;

					// Longitude
					case 10:
						longitude = Double.parseDouble(field);
						//System.out.println("longitude = " + longitude);
						break;

					// Latitude
					case 11:
						latitude = Double.parseDouble(field);
						//System.out.println("latitude = " + latitude);
						break;
				}
			}

			// Discard invalid inputs
			if (uTags == null) {
				return;
			}

			Country country = Country.getCountryAt(latitude, longitude);

			if (country != null) {
				//System.out.println("mappage de " + country.toString() + ", tags : " + uTags);
				context.write(new Text(country.toString()), new Text(uTags));
			}
		}
	}

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyReducer extends Reducer<Text, Text, Text, MinMaxPriorityQueue> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> tagsMap = new HashMap<>();

			for (Text value : values) {
				for (String tag : value.toString().split(",")) {
					Integer oCount = tagsMap.get(tag);
					int count = 0;

					if (oCount != null) {
						count = oCount.intValue();
					}

					tagsMap.put(tag, count + 1);
				}
			}

			Configuration config = context.getConfiguration();
			final int K = config.getInt("K", K_DEFAULT);

			MinMaxPriorityQueue<StringAndInt> pqueue = MinMaxPriorityQueue.maximumSize(K).create();

			for (Map.Entry<String, Integer> entry : tagsMap.entrySet()) {
				String tag = entry.getKey();
				int count = entry.getValue();

				pqueue.add(new StringAndInt(tag, count));
			}

			HashMap<String, Integer> oTags = new HashMap<>();

			context.write(key, pqueue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String input;
		String output;

		if (otherArgs.length < 2) {
			output = "output";

			if (otherArgs.length < 1) {
				input = "input";
			}
            else {
                input = otherArgs[0];
            }
		}
        else {
            output = otherArgs[1];
            input = otherArgs[0];

			if (otherArgs.length >= 3) {
				conf.set("K", otherArgs[2]);
			}
        }
		
		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxPriorityQueue.class);

//      job.setCombinerClass(WordCountCombiner.class);
//      job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
