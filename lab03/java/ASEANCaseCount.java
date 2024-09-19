import java.io.IOException; 
import java.text.NumberFormat; //package test.lab03.CumSum; could import at begining
import java.text.ParseException;
import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ASEANCaseCount {

	public static long convertToLong(String currencyString) {
		try {
		    NumberFormat format = NumberFormat.getInstance();
		    Number number = format.parse(currencyString);
		    return number.longValue();
		} catch (ParseException e) {
		    e.printStackTrace();
		    return 0;
		}
	}

	public static class FilteringMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private final String keyRegion = "South-East Asia";
		public void map(Object key, Text value, Context context) 
							throws IOException, InterruptedException {
		    // Value is line
		    String[] tokens = value.toString().split("\t");
			try {
				if (tokens.length > 1) {
				    String region = tokens[1];
				    if (region.equals(keyRegion))
				        context.write(new Text(keyRegion), 
						new LongWritable(convertToLong(tokens[2])));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
						throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values)
				sum += val.get();
			context.write(key, new LongWritable(sum));
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CumSum");
        job.setJarByClass(ASEANCaseCount.class);

        job.setMapperClass(FilteringMapper.class);
        job.setReducerClass(SumReducer.class);

      
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

