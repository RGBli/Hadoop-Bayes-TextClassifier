import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    //训练及测试选取的类别
    public static String[] CLASS_NAMES = {"AUSTR", "CANA"};
    //public static String[] CLASS_NAMES={"ALB"};
    //训练输入父目录
    public static String INPUT_PATH = "/bayes/data/train/";
    //类别统计输出目录
    public static String OUTPUT_PATH = "/bayes/output1/";
    //class-term统计输出目录
    public static String OUTPUT_PATH1 = "/bayes/output2/";
    //最终TEST测试分类结果输出目录
    public static String OUTPUT_PATH2 = "/bayes/output3/";
    //Test测试分类输入文件父目录，具体测试文件在该目录下具体类别的子目录
    public static String INPUT_PATH_TEST = "/bayes/data/test/";


    private static Pattern testClassnamePattern = Pattern.compile("test/(.*)/");

    public static String getTestClassname(String text) {
        Matcher matcher = testClassnamePattern.matcher(text);
        System.out.println(text);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    //从路径中抽取文件docId
    private static Pattern filenamePattern = Pattern.compile("/(\\w*).txt");

    public static String getFilename(String text) {
        Matcher matcher = filenamePattern.matcher(text);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
