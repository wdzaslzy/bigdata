package top.wdzaslzy.bigdata.solr;

import top.wdzaslzy.bigdata.util.OkHttpUtil;

/**
 * @author zhongyou_li
 */
public class UpdateSolrSchema {

    private static String SOLR_JAVA_DEMO_URL = "http://localhost:8983/solr/JavaDemo/schema";

    public static void main(String[] args) {
        String requestJson = "{\n"
                + "    \"add-field\":{\n"
                + "        \"name\":\"test_field\",\n"
                + "        \"type\":\"text_ik\",\n"
                + "        \"indexed\":true,\n"
                + "        \"stored\":true\n"
                + "    }\n"
                + "}";
        OkHttpUtil.sendPostRequestForJson(SOLR_JAVA_DEMO_URL, requestJson);
    }

}
