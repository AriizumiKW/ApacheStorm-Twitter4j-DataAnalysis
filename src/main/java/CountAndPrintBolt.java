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

/**
 * Count number and print it into log file and record file.
 * Mainly some simple statistical works.
 */
public class CountAndPrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2611275076211494119L;
	private FileWriter recordWritter;
	private FileWriter logWriter;
	private BufferedReader recordReader;

	private int countCONSER;
	private int countLABOUR;
	private int countLIBDEM;
	private int countBREXIT;
	// count the number of tweets with positive sentiment, for four political parties.
	// contain historical data

	private int totalNumOfCONSERTweets;
	private int totalNumOfLABOURTweets;
	private int totalNumOfLIBDEMTweets;
	private int totalNumOfBREXITTweets;
	// count the total number of tweets, for four political parties
	// not contain historical data

	private int positiveNumOfCONSERTweets;
	private int positiveNumOfLABOURTweets;
	private int positiveNumOfLIBDEMTweets;
	private int positiveNumOfBREXITTweets;
	// count the number of tweets with positive sentiment, for four political parties.
	// not contain historical data
	
	private int loopCount;
	// loopCount

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		try {
			logWriter = new FileWriter("log.csv", true);
			recordReader = new BufferedReader(new FileReader("record.csv"));
			readHistoryRecord(); // read historical data
			recordReader.close(); // close the recordReader
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
		count(category, sentiment); // corresponding count number + 1
		
		loopCount += 1;
		try {
			makeRecord(); // make record
			if(loopCount >= 1000) {
				makeLog(); // make log for every 1000 tweets
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
			logWriter.close(); // close logWritter
			recordWritter.close(); // close recordWritter
		} catch (IOException e) {
			e.printStackTrace();
		}
		super.cleanup();
	}

	private void makeRecord() throws IOException {
		// record historical number
		recordWritter.close();
		recordWritter = new FileWriter("record.csv",false); // empty the file
		String[] records = { "CONSER," + countCONSER, "LABOUR," + countLABOUR, "LIBDEM," + countLIBDEM,
				"BREXIT," + countBREXIT };
		for (String record : records) {
			recordWritter.write(record + '\n'); // make record
		}
		recordWritter.flush();
	}
	
	private void makeLog() throws IOException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd|HH:mm:ss"); // get date and time
		String dateStr = dateFormat.format(new Date()); // transform it into String type
		double posiSentiRateOfCONSER = (double) positiveNumOfCONSERTweets / totalNumOfCONSERTweets;
		double posiSentiRateOfLABOUR = (double) positiveNumOfLABOURTweets / totalNumOfLABOURTweets;
		double posiSentiRateOfLIBDEM = (double) positiveNumOfLIBDEMTweets / totalNumOfLIBDEMTweets;
		double posiSentiRateOfBREXIT = (double) positiveNumOfBREXITTweets / totalNumOfBREXITTweets;
		// get the rate of positive sentiment

		String outputStr = dateStr + "," + posiSentiRateOfCONSER + "," + posiSentiRateOfLABOUR + ","
				+ posiSentiRateOfLIBDEM + "," + posiSentiRateOfBREXIT + "," + countCONSER + "," + 
				countLABOUR + "," + countLIBDEM + "," + countBREXIT;
		logWriter.write(outputStr + '\n'); // write to log file
		logWriter.flush();
	}

	private void count(int category, int sentiment) {
		// if this tweet has positive sentiment, corresponding count number + 1
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
		// read from historical record file, and initialize countCONSER,countLABOUR,countLIBDEM,countBREXIT
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
