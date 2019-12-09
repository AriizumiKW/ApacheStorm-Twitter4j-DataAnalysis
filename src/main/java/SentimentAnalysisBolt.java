import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentimentAnalysisBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	
	public static final int POSITIVE = 1;
	public static final int NEGATIVE = 2;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		int category = input.getIntegerByField("category");
		String[] wordsLists = (String[]) input.getValueByField("wordslist");
		int sentiment = POSITIVE;
		
		collector.emit(new Values(category,sentiment,wordsLists));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category","sentiment","wordslist"));
	}

}
