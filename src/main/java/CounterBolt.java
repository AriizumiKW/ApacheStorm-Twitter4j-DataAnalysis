import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bolt filters out a predefined set of words.
 * @author davidk
 */
public class CounterBolt extends BaseRichBolt {
	
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	int category = input.getIntegerByField("category");
    	int sentiment = input.getIntegerByField("sentiment");
    	String word = Arrays.toString((String[])input.getValueByField("wordslist"));
    	collector.emit(new Values(category+","+sentiment+","+word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("context"));
    }
}
