import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentimentAnalysisBolt extends BaseRichBolt {

	private static final long serialVersionUID = 736984297988795694L;
	private OutputCollector collector;
	private BufferedReader positiveSentiProbReader;
	private BufferedReader negativeSentiProbReader;
	private HashMap<String, Integer> positiveSentiMap;
	private HashMap<String, Integer> negativeSentiMap;

	public static final int NEGATIVE_SENTIMENT = 0;
	public static final int POSITIVE_SENTIMENT = 1;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		positiveSentiMap = new HashMap<String, Integer>();
		negativeSentiMap = new HashMap<String, Integer>();
		try {
			positiveSentiProbReader = new BufferedReader(new FileReader("positive wordcount.csv"));
			negativeSentiProbReader = new BufferedReader(new FileReader("negative wordcount.csv"));
			positiveSentiProbReader.readLine(); // ignore the first line
			negativeSentiProbReader.readLine(); // ignore thr first line
			initHashMap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		int category = input.getIntegerByField("category");
		String[] wordsList = (String[]) input.getValueByField("wordslist");
		int sentiment = this.compareLikelihood(wordsList.clone());

		collector.emit(new Values(category, sentiment, wordsList));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "sentiment", "wordslist"));
	}

	private void initHashMap() throws IOException {
		while (true) {
			String nextLine = positiveSentiProbReader.readLine();
			if (nextLine == null) {
				break;
			}
			String[] strs = nextLine.split(",");
			positiveSentiMap.put(strs[0], Integer.parseInt(strs[1]));
		}
		while (true) {
			String nextLine = negativeSentiProbReader.readLine();
			if (nextLine == null) {
				break;
			}
			String[] strs = nextLine.split(",");
			negativeSentiMap.put(strs[0], Integer.parseInt(strs[1]));
		}
	}

	private int compareLikelihood(String[] wordsList) {
		/*
		 * the likelihood here is not the real likelihood P(wordsList|A) 
		 * 1. Ignore the relation between one word and another word. 
		 *   (E.g. "I" usually appears together with "am". "He" usually appears together
		 *   with "is".) 
		 * 2. Ignore the denominator of P(W(n)|A).
		 *   (E.g. "assignment" appears 30 times in negative sentiment. Total number of
		 *   word is 30000. I just ignore total number. So P(assignment|negative) = 30,
		 *   rather than 30/30000. It will not affect conclusion because the other has the
		 *   same number of factors.
		 * 3. If one number is too big (>=10000), then both this and the other divided by
		 * 10000. It is to prevent number out of range. It will not affect the conclusion.
		 */
		double posiLikelihood = 1;
		double negaLikelihood = 1;
		for (String word : wordsList) {
			Object positiveCount = positiveSentiMap.get(word);
			if (positiveCount == null) {
				positiveCount = 1;
				// The word doesn't exist, so ignore it. (*1 will not change anything)
			}
			Object negativeCount = negativeSentiMap.get(word);
			if (negativeCount == null) {
				negativeCount = 1;
				// The word doesn't exist, so ignore it. (*1 will not change anything)
			}
			posiLikelihood *= (Integer) positiveCount;
			negaLikelihood *= (Integer) negativeCount;

			if (posiLikelihood > 10000 || negaLikelihood > 10000) {
				posiLikelihood /= 10000;
				negaLikelihood /= 10000;
			}
		}
		if (posiLikelihood >= negaLikelihood) {
			return POSITIVE_SENTIMENT;
		} else {
			return NEGATIVE_SENTIMENT;
		}
	}

}
