package top.wdzaslzy.bigdata.solr

import java.io._
import java.util.Date

import com.opencsv.CSVReaderBuilder
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @author zhongyou_li
  */
object AddDocument {

  def main(args: Array[String]): Unit = {
    read()
  }

  def read(): Unit = {
    val file = new File("E:\\company.csv")
    val csvReader = new CSVReaderBuilder(new BufferedReader(new InputStreamReader(new FileInputStream(new File("E:\\company.csv")), "utf-8"))).build()
    val iterator = csvReader.iterator()

    val buffer = new ArrayBuffer[SolrInputDocument]()
    var index = 0

    while (iterator.hasNext) {
      val arr = iterator.next()
      index += 1
      if (index > 685738) {
        val companyName = arr(0)
        val tuple = if (companyName != null && !companyName.isEmpty && !companyName.contains("company_name") && arr.length > 3) {
          val companyUniqueCode = arr(1)
          val name = arr(3)
          val names = Array(name)
          val captial = Random.nextInt(100) + 1
          (companyUniqueCode, companyUniqueCode, companyName, null, names, new Date().getTime, captial)
        } else {
          ("123456", "123456", "测试企业名称", null, Array("测试"), new Date().getTime, 1)
        }

        val document = new SolrInputDocument()
        document.addField("id", tuple._1)
        document.addField("company_unique_code", tuple._2)
        document.addField("company_name", tuple._3)
        document.addField("brand_names", tuple._4)
        document.addField("legal_person_names", tuple._5)
        document.addField("establishment_date", tuple._6)
        document.addField("registered_capital", tuple._7)

        buffer += document

        var x = 1
        if (buffer.length == 50000) {
          println(s"发送了$x 次请求")
          val solrClient = new HttpSolrClient.Builder("http://localhost:8983/solr/CompanyInfo").build()
          commit(solrClient, buffer)
          buffer.clear()
          solrClient.close()
          x += 1
        }
      }
    }

  }

  def commit(solrClient: HttpSolrClient, buffer: ArrayBuffer[SolrInputDocument]): Unit = {
    try {
      solrClient.add(buffer.asJava)
      solrClient.commit(true, true, true)
    } catch {
      case e@(_: SolrServerException | _: IOException) =>
        e.printStackTrace()
    }
  }

}
