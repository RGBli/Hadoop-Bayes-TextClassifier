import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    //训练及测试选取的类别
    public static String[] CLASS_NAMES = {"AUSTR", "CANA"};
    //public static String[] CLASS_NAMES={"ALB"};
    //训练输入父目录
    public static String INPUT_PATH = "/bayes/data/train/";
    //类别统计输出目录
    public static String OUTPUT_PATH1 = "/bayes/output1/";
    //class-term统计输出目录
    public static String OUTPUT_PATH2 = "/bayes/output2/";
    //最终TEST测试分类结果输出目录
    public static String OUTPUT_PATH3 = "/bayes/output3/";
    //Test测试分类输入文件父目录，具体测试文件在该目录下具体类别的子目录
    public static String INPUT_PATH_TEST = "/bayes/data/test/";


    //从路径中抽取类别名称
    private static Pattern classnamePattern = Pattern.compile("train/(.*)/");

    public static String getClassname(String text) {
        Matcher matcher = classnamePattern.matcher(text);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
