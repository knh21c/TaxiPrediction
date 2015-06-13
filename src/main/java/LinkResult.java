import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class LinkResult {
	public static class MyMapper_TopN extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// 입력파일을 그대로 리듀스로 출력(T_Link값, 총 승차횟수)
			context.write(key, value);
		}
	}
	
	public static class MyMapper_Mapping extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 입력 라인을 ','를 기준으로 분할
			String line = value.toString();
			String[] inputs = line.split(",");
			// 분할한 배열의 두 번째 값인 T_Link값을 키로, 표준 링크 ID를 value로 출력
			context.write(new Text(inputs[1]), new Text(inputs[0]));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Values의 데이터 개수는 1개 또는 2개
			String link = "";
			String cnt_on = "";
			Iterator<Text> it = values.iterator();
			while(it.hasNext()){
				String tmp = it.next().toString();
				// 문자열의 길이가 10이면 표준 링크 ID 그렇지 않으면 총 승차횟수
				if(tmp.toString().length() == 10)
					link = tmp;
				else
					cnt_on = tmp;
			}
			if(!cnt_on.equals(""))
				context.write(new Text(link), new Text(cnt_on));
		}
	}
}
