import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		try {
			logWriter = new FileWriter("log.csv", true);
			recordWritter = new FileWriter("record.csv");
			recordReader = new BufferedReader(new FileReader("record.csv"));
			readHistoryRecord();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		int category = input.getIntegerByField("category");
		int sentiment = input.getIntegerByField("sentiment");
		if (sentiment == SentimentAnalysisBolt.POSITIVE) {
			count(category);
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
		String[] records = { "CONSER," + countCONSER, "LABOUR," + countLABOUR, "LIBDEM," + countLIBDEM,
				"BREXIT" + countBREXIT };
		for (String record : records) {
			recordWritter.write(record + '\n');
		}
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
	}

	private void count(int category) {
		// corresponding count number + 1
		switch (category) {
		case TweetClassificationBolt.CONSER:
			countCONSER += 1;
			break;
		case TweetClassificationBolt.LABOUR:
			countLABOUR += 1;
			break;
		case TweetClassificationBolt.LIBDEM:
			countLIBDEM += 1;
			break;
		case TweetClassificationBolt.BREXIT:
			countBREXIT += 1;
			break;
		default:
			break;
		}
	}

	private void readHistoryRecord() throws IOException {
		String countStr = recordReader.readLine().split(",")[1];
		countCONSER = Integer.parseInt(countStr);
		countStr = recordReader.readLine().split(",")[1];
		countLABOUR = Integer.parseInt(countStr);
		countStr = recordReader.readLine().split(",")[1];
		countLIBDEM = Integer.parseInt(countStr);
		countStr = recordReader.readLine().split(",")[1];
		countBREXIT = Integer.parseInt(countStr);

		recordReader.close();
	}
}
