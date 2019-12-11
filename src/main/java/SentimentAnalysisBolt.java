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

/**
 * Analyze each tweet. That is, estimate whether the tweet has positive sentiment or negative sentiment.
 * I use Naive Bayes algorithm to train and test my model. And the model has already been trained well.
 * It is "positive wordcount.csv" and "negative wordcount.csv".
 */
public class SentimentAnalysisBolt extends BaseRichBolt {

	private static final long serialVersionUID = 736984297988795694L;
	private OutputCollector collector;
	private BufferedReader positiveSentiCountReader;
	private BufferedReader negativeSentiCountReader; // read from word occurrence number file
	private HashMap<String, Integer> positiveSentiMap;
	private HashMap<String, Integer> negativeSentiMap;
	// string: the word.  Integer: occurrence number when training

	public static final int NEGATIVE_SENTIMENT = 0;
	public static final int POSITIVE_SENTIMENT = 1;
	// tag, used to note each tweet positive sentiment or negative sentiment

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		positiveSentiMap = new HashMap<String, Integer>();
		negativeSentiMap = new HashMap<String, Integer>();
		try {
			positiveSentiCountReader = new BufferedReader(new FileReader("positive wordcount.csv"));
			negativeSentiCountReader = new BufferedReader(new FileReader("negative wordcount.csv"));
			positiveSentiCountReader.readLine(); // ignore the first line
			negativeSentiCountReader.readLine(); // ignore the first line
			initHashMap(); // initialize HashMap
			positiveSentiCountReader.close(); // close positive sentiment count reader
			negativeSentiCountReader.close(); // close negative sentiment count reader
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		int category = input.getIntegerByField("category");
		String[] wordsList = (String[]) input.getValueByField("wordslist");
		
		int sentiment = this.compareLikelihood(wordsList.clone());
		// Estimate whether a tweet has positive sentiment or negative sentiment

		collector.emit(new Values(category, sentiment, wordsList));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "sentiment", "wordslist")); // declare output field
	}

	private void initHashMap() throws IOException {
		while (true) {
			String nextLine = positiveSentiCountReader.readLine();
			if (nextLine == null) { // if it arrives the end of file, then terminate.
				break;
			}
			String[] strs = nextLine.split(",");
			positiveSentiMap.put(strs[0], Integer.parseInt(strs[1])); 
			// find the value, put it into HashMap
		}
		while (true) {
			String nextLine = negativeSentiCountReader.readLine();
			if (nextLine == null) { // if it arrives the end of file, then terminate.
				break;
			}
			String[] strs = nextLine.split(",");
			negativeSentiMap.put(strs[0], Integer.parseInt(strs[1]));
			// find the value, put it into HashMap
		}
	}

	private int compareLikelihood(String[] wordsList) {
		/*
		 * Estimate whether a tweet has positive sentiment or negative sentiment.
		 * 
		 * the likelihood here is not the real likelihood P(wordsList|A) 
		 * 1. Ignore the relation between one word and another word. So I can use Naive-Bayes.
		 *   (E.g. "you" usually appears together with "are". "He" usually appears together
		 *   with "is".) 
		 * 2. Ignore the denominator of P(W(n)|A).
		 *   (E.g. "assignment" appears 30 times in negative sentiment file. Total number of
		 *   word is 30000. I just ignore the total number. So P(assignment|negative) = 30,
		 *   rather than 30/30000. It will not affect conclusion because the other has the
		 *   same number of denominators.)
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
				// if one number is too big, both this and the other divided by 10000.
				// It will not affect the result, because we just compare them with each other.
				posiLikelihood /= 10000;
				negaLikelihood /= 10000;
			}
		}
		if (posiLikelihood >= negaLikelihood) {
			// if the likelihood of positive sentiment is much bigger.
			// Then this tweet has positive sentiment.
			return POSITIVE_SENTIMENT;
		} else {
			// Vice versa
			return NEGATIVE_SENTIMENT;
		}
	}

}
