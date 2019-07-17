package top.wdzaslzy.bigdata.solr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient.Builder;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author zhongyou_li
 */
public class AddIndexData {

    private static String SOLR_JAVA_DEMO_URL = "http://bigdata-3.baofoo.cn:8983/solr/TrademarkInfo";

    public static void main(String[] args) throws IOException, SolrServerException {
        businessIndex();
        /*List<SolrInputDocument> documentList = new ArrayList<>();

        String path = "E:\\test\\query-hive-197453.xlsx";
        String fileType = path.substring(path.lastIndexOf(".") + 1);
        //读取excel文件
        InputStream is = null;

        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "8134c06e11e15b08fe97efe0c767dc82");
        document.addField("brand_name", "新颜科技");
        document.addField("company_name", "上海新颜人工智能科技有限公司");

        SolrInputDocument document1 = new SolrInputDocument();
        document1.addField("id", "7c73aa1e92723e9408ee39a880ae1a67");
        document1.addField("brand_name", "支付宝");
        document1.addField("company_name", "阿里巴巴集团控股有限公司");

        SolrInputDocument document2 = new SolrInputDocument();
        document2.addField("id", "ddd112bd05fd604f709604989465bcaa");
        document2.addField("brand_name", "华为");
        document2.addField("company_name", "华为技术有限公司");

        SolrInputDocument document3 = new SolrInputDocument();
        document3.addField("id", "afba1be845964f2744838a9c2f3378fb");
        document3.addField("brand_name", "小米");
        document3.addField("company_name", "小米科技有限责任公司");

        SolrInputDocument document4 = new SolrInputDocument();
        document4.addField("id", "c67746842aecb2f3be6c31d302f3b96d");
        document4.addField("brand_name", "京东商城");
        document4.addField("company_name", "北京京东叁佰陆拾度电子商务有限公司");

        SolrInputDocument document5 = new SolrInputDocument();
        document5.addField("id", "22a83c30895e2d103aeb2fc46507dd11");
        document5.addField("brand_name", "天猫");
        document5.addField("company_name", "浙江天猫网络有限公司");

        documentList.add(document);
        documentList.add(document1);
        documentList.add(document2);
        documentList.add(document3);
        documentList.add(document4);
        documentList.add(document5);

        ConcurrentUpdateSolrClient solrClient = new ConcurrentUpdateSolrClient.Builder(
                SOLR_JAVA_DEMO_URL).build();

        try {
            solrClient.add(documentList);
            solrClient.commit(true, true, true);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }*/


       /* HttpSolrClient build = new Builder(SOLR_JAVA_DEMO_URL).build();
        build.add(documentList);
        build.commit();*/

//        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();

    }

    public static void businessIndex() throws IOException, SolrServerException {

        List<SolrInputDocument> documentList = new ArrayList<>();

        String path = "E:\\query-hive-210564.xlsx";
        String fileType = path.substring(path.lastIndexOf(".") + 1);
        FileInputStream inputStream = new FileInputStream(new File(path));
        //获取工作薄
        Workbook wb = null;
        if (fileType.equals("xls")) {
            wb = new HSSFWorkbook(inputStream);
        } else if (fileType.equals("xlsx")) {
            wb = new XSSFWorkbook(inputStream);
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        Sheet sheet = wb.getSheetAt(0);
        Iterator<Row> it = sheet.rowIterator();
        while (it.hasNext()) {
            Row row = it.next();
            Cell cell0 = row.getCell(0);
            Cell cell1 = row.getCell(1);
            Cell cell3 = row.getCell(3);
            Cell cell4 = row.getCell(4);
            Cell cell6 = row.getCell(6);

            Object[] objects = new Object[100];
            if (cell3 != null) {
                String names = cell3.getStringCellValue();
                if (names != null && !names.trim().isEmpty()) {
                    JSONArray array = JSON.parseArray(names);
                    objects = array.toArray();
                }
            }

            long l = 0L;
            if (cell4 != null) {
                if (Cell.CELL_TYPE_STRING == cell4.getCellType()) {
                    String replace = cell4.getStringCellValue().trim().replace(",", "");
                    l = (long) (Double.parseDouble(replace) * 1000L);
                } else if (Cell.CELL_TYPE_NUMERIC == cell4.getCellType()) {
                    l = (long) (cell4.getNumericCellValue() * 1000L);
                }
            }

            long dates = 0L;
            if (cell6 != null) {
                String date = cell6.getStringCellValue();
                try {
                    dates = df.parse(date).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", cell1.getStringCellValue());
            document.addField("company_unique_code", cell1.getStringCellValue());
            document.addField("organization_code", cell1.getStringCellValue());
            document.addField("company_name", cell0.getStringCellValue());
            document.addField("company_name_str", cell0.getStringCellValue());
            document.addField("brand_names", Lists.newArrayList());
            document.addField("legal_person_names", objects);
            document.addField("establishment_date", dates);
            document.addField("registered_capital", l);

            documentList.add(document);

        }

        SolrInputDocument document = new SolrInputDocument();

        document.addField("id", "91140105MA0sdfHFYLF3E");
        document.addField("company_unique_code", "91140105MA0HFYLF3E");
        document.addField("organization_code", "MA0HFYLF3");
        document.addField("company_name", "山西唯科亿达科技有限公司");
        document.addField("company_name_str", "山西唯科亿达科技有限公司");
        document.addField("brand_names", Lists.newArrayList());
        document.addField("legal_person_names", Lists.newArrayList("郭东亮"));
        document.addField("establishment_date", 1494950400000L);
        document.addField("registered_capital", 1000000L);


//        HttpSolrClient client = new HttpSolrClient.Builder(
//                "http://bigdata-3.baofoo.cn:8983/solr/CompanyBusinessCoreInfoV2").build();
//
//        client.add(document);
//        client.commit();
        ConcurrentUpdateSolrClient client = new Builder("http://bigdata-3.baofoo.cn:8983/solr")
                .build();
        client.add("CompanyBusinessCoreInfoV2", documentList);
        client.commit("CompanyBusinessCoreInfoV2", true, true, true);
        client.close();


    }

}
