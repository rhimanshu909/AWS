package com.cs.mapreduce.logprocessor;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author Ajinkya
 * This is a mapper class 
 */
public class LogAnalyzerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println(" Start the LogAnalyzerMapper ...");
		String line = value.toString();
		String switchName = "default";

		if (line.contains("successfully logged in")) {
			switchName = "LOGIN";

		} else if (line.contains("requested to open an account")) {
			switchName = "ACCOUNT_OPEN";

		} else if (line.contains("requested to close an account")) {
			switchName = "ACCOUNT_CLOSE";

		} else if (line.contains("requested for account statement")) {
			switchName = "ACCOUNT_STATEMENT";

		} else if(line.contains("requested to transfer money")) {
			switchName = "MONEY_TRANSFER";
		}

		processEvents(context, switchName);
	}

	private void processEvents(Context context, String switchName) throws IOException, InterruptedException {
		switch (switchName) {

		case "LOGIN":
			word.set(switchName);
			System.out.println(" pushing the key :LOGIN - value:" + one);
			context.write(word, one);
			break;

		case "ACCOUNT_OPEN":
			word.set(switchName);
			System.out.println(" pushing the key :ACCOUNT_OPEN - value:" + one);
			context.write(word, one);
			break;

		case "ACCOUNT_CLOSE":
			word.set(switchName);
			System.out.println(" pushing the key :ACCOUNT_CLOSE - value:" + one);
			context.write(word, one);
			break;

		case "ACCOUNT_STATEMENT":
			word.set(switchName);
			System.out.println(" pushing the key :ACCOUNT_STATEMENT - value:" + one);
			context.write(word, one);
			break;
		case "MONEY_TRANSFER":
			word.set(switchName);
			System.out.println(" pushing the key :MONEY_TRANSFER - value:" + one);
			context.write(word, one);
			break;

		default:
			System.out.println("default case !");
			break;
		}
	}

}
