package eRFactor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.log4j.Logger;

public class RFactorCombiner {
	public static class MapProcess extends Mapper<LongWritable, Text, Text, Pair> {
		private final int GEOGRAPH_FIELD = 56;
		private final int DATE_FIELD = 3;
		private final int FACTOR_R_FIELD = 20;  //Factor R column field number in file
		private Logger logger = Logger.getLogger(MapProcess.class);
		private Map<String, Pair> FactorsByOrigin;

		@Override
		protected void setup( Mapper<LongWritable, Text, Text, Pair>.Context context) throws IOException, InterruptedException {
			logger.info("==== FACTOR R COMBINER SETUP ====");
			FactorsByOrigin = new HashMap<String, Pair>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//logger.info("==== FACTOR R COMBINER MAP ====");
			if (value == null || value.toString().startsWith("id"))  //First line, omit it
				return;
			String line = value.toString();
			String[] chunks = line.split("\\t");  // Si es tab: \\t
			//logger.info("Record info: "+line);
			String initDate = chunks[DATE_FIELD].substring(0, 10);
			String id = chunks[GEOGRAPH_FIELD] + "_" + initDate;     //Geography location
			String measureChunk = chunks[FACTOR_R_FIELD]; //Factor R value
			//logger.info("R-Factor: " + measureChunk);
			Double number = 0.0;

			try {
				number = Double.parseDouble(measureChunk.replace(",", "."));
			} catch (NumberFormatException e) { }
			
			if (number==0||number > 100) //Remove outliers
				return;
			
			Pair p = null;
			if (!FactorsByOrigin.containsKey(id)) {
				p = new Pair(number, 1l);
			} else {
				p = FactorsByOrigin.get(id);
				p.setKey(p.getKey() + number);
				p.setValue(p.getValue() + 1);
			}
			FactorsByOrigin.put(id, p);
		}

		@Override
		protected void cleanup(	Mapper<LongWritable, Text, Text, Pair>.Context context)	throws IOException, InterruptedException {
			//logger.info("==== FACTOR R CLEANUP ====");
			for (Entry<String, Pair> entry : FactorsByOrigin.entrySet()) {
				context.write(new Text(entry.getKey()), entry.getValue());
			}
		}
	}

	public static class ReduceProcess extends Reducer<Text, Pair, Text, Pair> {
		
		public void reduce(Text key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			//logger.info("==== FACTOR R REDUCER ====");
			double sum = 0;
			long count = 0;
			for (Pair val : values) {
				sum += val.getKey();
				count += val.getValue();
			}
			//context.write(key, new DoubleWritable(sum / count));
			context.write(key, new Pair(sum/count,count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

	    System.setProperty("hadoop.home.dir", "/");
		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "RFactorInMapCombiner");
		job.setJarByClass(RFactorCombiner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputValueClass(Pair.class);

		job.setMapperClass(MapProcess.class);
		job.setReducerClass(ReduceProcess.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}