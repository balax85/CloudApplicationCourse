package mp1;
import java.io.BufferedReader;
import java.io.FileReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

public class MP1 {
	
	class Word implements Comparable<Word>{
		private String word;
		private Integer number;
		public Word(String word, Integer number) {
			this.word = word;
			this.number = number;
		}
		public String getWord() {
			return word;
		}
		public void setWord(String word) {
			this.word = word;
		}
		public Integer getNumber() {
			return number;
		}
		public void setNumber(Integer number) {
			this.number = number;
		}
		@Override
		public int compareTo(Word o) {
			if (this.number < o.getNumber()) return 1;
			if (this.number > o.getNumber()) return -1;
			return this.word.compareTo(o.getWord());
		}
	}
	
    Random generator;
    String userName;
    String inputFileName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];
       
        //TODO
        
        Map<String, Integer> mapResult = new HashMap<String, Integer>();
        ArrayList<String> lineList = new ArrayList<String>();
        
        FileReader a = new FileReader(inputFileName);
        BufferedReader br = new BufferedReader(a);
        String line;
        line = br.readLine();
        
        while (line != null) {
        	lineList.add(line);
        	line = br.readLine();
        }

        Integer[] indexes = getIndexes();
        
        for (Integer index : indexes ) {
        	
        	String l = lineList.get(index);
        	
        	StringTokenizer stk = new StringTokenizer(l, delimiters);
        	int i = stk.countTokens();
        	
        	while (stk.hasMoreTokens()) {
        		String token = stk.nextToken();
        		if (token != null) {
        			token = token.toLowerCase().trim();
        			if (!Arrays.asList(stopWordsArray).contains(token)) {
        				if (mapResult.containsKey(token)) {
        					Integer num = mapResult.get(token);
        					num++;
        					mapResult.remove(token); 
        					mapResult.put(token, num);
        				} else {
        					mapResult.put(token, 1);
        				}
        			}
        		}
        	}
        }
        
       List<MP1.Word> listWord = new ArrayList<MP1.Word>();
       for (String key : mapResult.keySet()) {
    	   Integer number = mapResult.get(key);
    	   listWord.add(new Word(key, number));
       }
       
       Collections.sort(listWord);
       
//       for (Word word : listWord) {
//    	   System.out.println("Word : " + word.getWord() + " number : " + word.getNumber());
//       }
       
       int i = 0;
       while (i < 20) {
    	   ret[i] = listWord.get(i).getWord();
    	   i++;
       }
        

        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item: topItems){
                System.out.println(item);
            }
        }
    }
}
