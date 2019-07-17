package top.wdzaslzy.bigdata.poi;

import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.util.CellRangeAddress;

/**
 * @author zhongyou_li
 */
public class GenerateExcel {

    public static void main(String[] args) {
        HSSFWorkbook wb = new HSSFWorkbook();
        HSSFSheet sheet = wb.createSheet();
        HSSFRow row = sheet.createRow(1);
        HSSFCellStyle cellStyle = wb.createCellStyle();
        cellStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
        cellStyle.setAlignment(CellStyle.ALIGN_CENTER_SELECTION);
        row.createCell(0).setCellValue("企业变更信息");
        row.createCell(1).setCellValue("测试1");
        HSSFRow row2 = sheet.createRow(2);
        row2.createCell(1).setCellValue("测试2");
        HSSFRow row3 = sheet.createRow(3);
        row3.createCell(1).setCellValue("测试3");
        HSSFRow row4 = sheet.createRow(4);
        row4.createCell(1).setCellValue("测试4");
        HSSFRow row5 = sheet.createRow(5);
        row5.createCell(1).setCellValue("测试5");

        CellRangeAddress region = new CellRangeAddress(1, 5, 0, 0);
        sheet.addMergedRegion(region);

        try {
            FileOutputStream fsot = new FileOutputStream("E://text.xls");
            wb.write(fsot);
            fsot.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
