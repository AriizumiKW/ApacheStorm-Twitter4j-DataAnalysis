import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class CountAndPrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2611275076211494119L;
	private FileWriter recordWritter;
	private FileWriter logWriter;
	private BufferedReader recordReader;

	private int countCONSER;
	private int countLABOUR;
	private int countLIBDEM;
	private int countBREXIT;

	private int totalNumOfCONSERTweets;
	private int totalNumOfLABOURTweets;
	private int totalNumOfLIBDEMTweets;
	private int totalNumOfBREXITTweets;

	private int positiveNumOfCONSERTweets;
	private int positiveNumOfLABOURTweets;
	private int positiveNumOfLIBDEMTweets;
	private int positiveNumOfBREXITTweets;
	
	private int loopCount;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		try {
			logWriter = new FileWriter("log.csv", true);
			recordReader = new BufferedReader(new FileReader("record.csv"));
			readHistoryRecord();
			recordReader.close();
			recordWritter = new FileWriter("record.csv",false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		totalNumOfCONSERTweets = 0;
		totalNumOfLABOURTweets = 0;
		totalNumOfLIBDEMTweets = 0;
		totalNumOfBREXITTweets = 0;
		positiveNumOfCONSERTweets = 0;
		positiveNumOfLABOURTweets = 0;
		positiveNumOfLIBDEMTweets = 0;
		positiveNumOfBREXITTweets = 0;
		loopCount = 0;
	}

	@Override
	public void execute(Tuple input) {
		int category = input.getIntegerByField("category");
		int sentiment = input.getIntegerByField("sentiment");
		count(category, sentiment);
		
		loopCount += 1;
		try {
			makeRecord();
			if(loopCount >= 1000) {
				makeLog();
				loopCount = 0;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		// guarentee to be called when shutdown in local mode
		try {
			makeLog();
			makeRecord();
			logWriter.close();
			recordWritter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		super.cleanup();
	}

	private void makeRecord() throws IOException {
		recordWritter.close();
		recordWritter = new FileWriter("record.csv",false); // empty file
		String[] records = { "CONSER," + countCONSER, "LABOUR," + countLABOUR, "LIBDEM," + countLIBDEM,
				"BREXIT," + countBREXIT };
		for (String record : records) {
			recordWritter.write(record + '\n');
		}
		recordWritter.flush();
	}
	
	private void makeLog() throws IOException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm:ss");
		String dateStr = dateFormat.format(new Date());
		double posiSentiRateOfCONSER = (double) positiveNumOfCONSERTweets / totalNumOfCONSERTweets;
		double posiSentiRateOfLABOUR = (double) positiveNumOfLABOURTweets / totalNumOfLABOURTweets;
		double posiSentiRateOfLIBDEM = (double) positiveNumOfLIBDEMTweets / totalNumOfLIBDEMTweets;
		double posiSentiRateOfBREXIT = (double) positiveNumOfBREXITTweets / totalNumOfBREXITTweets;

		String outputStr = dateStr + "," + posiSentiRateOfCONSER + "," + posiSentiRateOfLABOUR + ","
				+ posiSentiRateOfLIBDEM + "," + posiSentiRateOfBREXIT + "," + countCONSER + "," + 
				countLABOUR + "," + countLIBDEM + "," + countBREXIT;
		logWriter.write(outputStr + '\n');
		logWriter.flush();
	}

	private void count(int category, int sentiment) {
		// corresponding count number + 1
		switch (category) {
		case TweetClassificationBolt.CONSER:
			if(sentiment == SentimentAnalysisBolt.POSITIVE_SENTIMENT) {
				countCONSER += 1;
				positiveNumOfCONSERTweets += 1;
			}
			totalNumOfCONSERTweets += 1;
			break;
		case TweetClassificationBolt.LABOUR:
			if(sentiment == SentimentAnalysisBolt.POSITIVE_SENTIMENT) {
				countLABOUR += 1;
				positiveNumOfLABOURTweets += 1;
			}
			totalNumOfLABOURTweets += 1;
			break;
		case TweetClassificationBolt.LIBDEM:
			if(sentiment == SentimentAnalysisBolt.POSITIVE_SENTIMENT) {
				countLIBDEM += 1;
				positiveNumOfLIBDEMTweets += 1;
			}
			totalNumOfLIBDEMTweets += 1;
			break;
		case TweetClassificationBolt.BREXIT:
			if(sentiment == SentimentAnalysisBolt.POSITIVE_SENTIMENT) {
				countBREXIT += 1;
				positiveNumOfBREXITTweets += 1;
			}
			totalNumOfBREXITTweets += 1;
			break;
		default:
			break;
		}
	}

	private void readHistoryRecord() throws IOException {
		String countStr = recordReader.readLine();
		System.out.println(countStr);
		countCONSER = Integer.parseInt(countStr.split(",")[1]);
		countStr = recordReader.readLine().split(",")[1];
		countLABOUR = Integer.parseInt(countStr);
		countStr = recordReader.readLine().split(",")[1];
		countLIBDEM = Integer.parseInt(countStr);
		countStr = recordReader.readLine().split(",")[1];
		countBREXIT = Integer.parseInt(countStr);
	}
}
