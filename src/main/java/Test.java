import com.ginobefunny.elasticsearch.plugins.synonym.service.Configuration;
import com.ginobefunny.elasticsearch.plugins.synonym.service.DynamicSynonymTokenFilter;
import com.ginobefunny.elasticsearch.plugins.synonym.service.SimpleSynonymMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Test {

    public static void main(String[] args) {
        Configuration config = new Configuration(true, true, new WhitespaceAnalyzer(), null, null, 10);

        SimpleSynonymMap tempSynonymMap = new SimpleSynonymMap(config);

        List<String> rules = new LinkedList<>();
        rules.add("jac ket,jacket");
        rules.add("poc ket,pocket");
        rules.add("swimdress,swim dress");
        rules.add("re de,red");
        rules.add("reddress=>red dress");
        rules.add("poc kets=>pockets");

        for (String rule: rules) {
            tempSynonymMap.addRule(rule);
        }


        String text = "jac ket";

        List<String> result = new LinkedList<>();

        Analyzer analyzer = config.getAnalyzer();
        try (TokenStream ts = analyzer.tokenStream("", text)) {
            TokenStream stream = new DynamicSynonymTokenFilter(ts, tempSynonymMap);

            CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {

                result.add(new String(termAtt.buffer(), 0, termAtt.length()));
            }

            stream.end();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
