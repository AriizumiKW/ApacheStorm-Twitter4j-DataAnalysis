import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class PrinterBolt extends BaseRichBolt{
	
	FileWriter fileWriter;
	
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	
		try {
			fileWriter = new FileWriter("record.txt", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
    	
    	String str = input.getStringByField("s3");
    	System.out.println(str);
    	
    	try {
			fileWriter.write(str);
			fileWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
