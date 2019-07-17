package top.wdzaslzy.bigdata.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * @author zhongyou_li
 */
public class ExcelReaderUtil {

    public static void readExcel(String path) {
        String fileType = path.substring(path.lastIndexOf(".") + 1);
        Map<String, String> map = new HashMap<>();
        //读取excel文件
        InputStream is = null;
        String key = null;
        try {
            is = new FileInputStream(path);
            //获取工作薄
            Workbook wb = null;
            if (fileType.equals("xls")) {
                wb = new HSSFWorkbook(is);
            } else if (fileType.equals("xlsx")) {
                wb = new XSSFWorkbook(is);
            } else {
                return;
            }

            //读取第一个工作页sheet
            Sheet sheet = wb.getSheetAt(0);
            for (Row row : sheet) {
                Cell cell1 = row.getCell(0);
                Cell cell2 = row.getCell(1);
                String catalog = cell1.getStringCellValue();
                if (catalog != null && !catalog.isEmpty()) {
                    key = catalog;
                }
                map.put(cell2.getStringCellValue(), key);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " ----> " + entry.getValue());
        }

    }

    public static void main(String[] args) {
        readExcel("E:\\行业分类.xlsx");
    }

}
