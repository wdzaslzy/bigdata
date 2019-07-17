package top.wdzaslzy.bigdata.solr;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Optional;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * @author zhongyou_li
 */
public class QueryForSolr {

    private static String SOLR_JAVA_DEMO_URL = "http://localhost:8983/solr/JavaDemo";

    public static void main(String[] args) throws IOException, SolrServerException {
        CloudSolrClient client = new CloudSolrClient.Builder(Lists.newArrayList("10.0.221.73:2181"),
                Optional.of("/solr")).build();
        client.setDefaultCollection("CompanyBusinessCoreInfo");
        SolrQuery query = new SolrQuery();
        query.set("q", "company_name:上海");
        QueryResponse response = client.query(query);
        SolrDocument entries = response.getResults().get(0);

    }

}
