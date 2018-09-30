package top.wdzaslzy.bigdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author lizy
 **/
public class Test {

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date before = dateFormat.parse("2018-09-01 10:00:00");
        Date end = dateFormat.parse("2018-09-01 10:00:00");
        System.out.println(before.before(end));
    }

}
