import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class TaxiPrediction{
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		private int day; //입력받은 요일 값을 저장할 변수
		private int time; //입력받은 시간 값을 저장할 변수

		// 변수에 입력받은 값 대입
		public void setup(Context context) throws IOException{
			day = context.getConfiguration().getInt("Day", 0);
			time = context.getConfiguration().getInt("Time", 00);
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			LongWritable mapOut;
			String line = value.toString();
			// 한 줄의 입력 데이터를 ','를 기준으로 분할
			String[] inputs = line.split(",");
			// 파일의 첫 줄은 실제 데이터가 아닌 필드 설명내용이기 때문에 Skip
			if(!inputs[1].equals("Day")){ 
				// 입력받은 요일, 시간과 동일한 데이터에 대해서만 처리
				if(Integer.parseInt(inputs[1]) == day && Integer.parseInt(inputs[2]) == time){
					// 승차횟수가 널일경우 0으로 셋
					if (inputs[5].equals("")) {
						mapOut = new LongWritable(0);
					} else {
						mapOut = new LongWritable(Integer.parseInt(inputs[5]));
					}
					
					context.write(new Text(inputs[0]), mapOut);
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			// 입력 value들을 모두 더한 후 출력
			for(LongWritable val : values){
				sum += val.get();
			}
			
			context.write(key, new LongWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		// 사용자가 입력한 Day, Time값 설정
		conf.setInt("Day", Integer.parseInt(args[2]));
		conf.setInt("Time", Integer.parseInt(args[3]));
		
		Job job = new Job(conf, "TaxiPrediction");
		
		job.setJarByClass(TaxiPrediction.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// 리듀스 태스크수 10으로 지정
		job.setNumReduceTasks(10);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		if(!job.waitForCompletion(true))
			return;
		
		
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "TopN");

		job2.setJarByClass(TopN.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);

		job2.setMapperClass(TopN.Map.class);
		job2.setReducerClass(TopN.Reduce.class);
		job2.setNumReduceTasks(1);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		// TaxiPrediction의 출력을 TopN의 입력으로 사용
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		// TopN을 위한 출력 경로 지정
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "//topN"));
		job2.getConfiguration().setInt("topN", 10);

		if (!job2.waitForCompletion(true))
			return;
		
		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "LinkResult");
		job3.setJarByClass(LinkResult.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		// 두 개의 입력파일에 각각 다른 맵 클래스를 지정해주기 때문에
		// setMapper필요 없음
		job3.setReducerClass(LinkResult.Reduce.class);
		
		// MultipleInputs 에 입력과 맵 클래스, 출력 포맷 설정
		MultipleInputs.addInputPath(job3, new Path(args[1] + "//topN"), 
				KeyValueTextInputFormat.class, LinkResult.MyMapper_TopN.class);
		MultipleInputs.addInputPath(job3, new Path(args[4]), 
				TextInputFormat.class, LinkResult.MyMapper_Mapping.class);
		
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "//LinkResult"));
		
		if(!job3.waitForCompletion(true))
			return;
	}
}
