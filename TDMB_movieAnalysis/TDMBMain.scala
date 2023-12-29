package bigdata.classes.TDMB_movieAnalysis


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.shaded.org.eclipse.jetty.util.ajax.JSON
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.PrintWriter
import org.json4s.JsonDSL._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.catalyst.expressions.Md5
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object TDMBMain {
    val conf = new SparkConf().setMaster("local").setAppName("TDMBMain")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession
        .builder()
        .appName("My Spark SQL")
        .getOrCreate()

    /**
     * 实现解析json格式字段从中提取出name并进行词频统计的功能
     * @param field
     * @param mdf
     * @return
     */
    def countByJson(field:String,mdf:DataFrame):org.apache.spark.rdd.RDD[(String,Int)]  ={
        val jsonSchema =ArrayType(new StructType().add("id", IntegerType).add("name",StringType))
        mdf.select(mdf.col(field))
            .filter(mdf.col(field).isNotNull)
            //     此处是单条中包含多个数据的json，按照jsonSchema的格式进行解析，并生成多条单个数据，explode是将数组组生成为列。
            .select(explode(from_json(mdf.col(field), jsonSchema)).as(field))
            //     解决$"genres.name"的变量问题
            .select(field.concat(".name"))
            .rdd
            .map(name=>(name.toString(),1))
            .repartition(1)
            .reduceByKey((x,y) => x + y)
    }


    /**
     * countByGenres 用来生成不同题材的电影数统计结果
     * @param mdf
     * @return
     */
    def countByGenres(mdf:DataFrame):String={
        val genresRDD = countByJson("genres",mdf)
        val jsonString = genresRDD.collect().toList.map{case (genre,count)=>
            (("genre" ->genre.replace("[","").replace("]",""))~("count" ->count))
        }
        val mdfJson = compact(render(jsonString))
        mdfJson
    }

    /**
     * 统计电影题材，生成[{"genre":xxx, "count":xxx},]格式json文件
     * @param mdf
     * @param spark
     */
    def countByGenres2(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_movie");
        val genreDF=sqlContext.sql("select genres from t_movie");

        //[{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}, {"id": 14, "name": "Fantasy"}, {"id": 878, "name": "Science Fiction"}]
        val genreRDD=genreDF.rdd
        val genreArr=genreRDD.filter(t=>t.getAs("genres").toString.contains("\"name\"")).map(x=>x.toString)
            .flatMap(_.split(",")).filter(x=>x.contains("\"name\""))
            .map{
                genre=>
                    val index=genre.indexOf("name\": \"");
                    val end=genre.indexOf("\"}");
                    genre.substring(index+8, end)
            }.distinct.collect

        // [{"genre":xxx, "count":xxx},]
        var str = ""

        for(genre <- genreArr){
            val countArray=sqlContext.sql("select count(genres) as genreCount from t_movie where genres like '%"+genre+"%'").collect();
            val endIndex = countArray(0).toString.length - 1
            str = str + "{\"genre\":\'"+ genre + "\', \"count\":"+countArray(0).toString().substring(1,endIndex) +"},"
        }
        str = "["+str+"]"
        str = str.replace(",]","]")
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/genres.json")
        str
    }

    /**
     * 统计Top100的常见关键字
     * @param mdf
     * @return
     */
    def countKeywords_Top100(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_keywords")
        val t_mdf = sqlContext.table("t_keywords")
        val keywordsDF = t_mdf.select("keywords")
        val keywordsRDD = keywordsDF.rdd
        val keywordsArr = keywordsRDD.filter(row=>row.getAs("keywords").toString.contains("\"name\"")).map(x=>x.toString())
            .flatMap(_.split("},")).filter(x=>x.contains("\"name\""))
            .map{
                keyword=>
                    val index = keyword.indexOf("name\": \"")
                    val end = keyword.lastIndexOf("\"")
                    keyword.substring(index+8,end)
            }.map(row=>(row,1)).reduceByKey(_+_)
        val countKeywords = keywordsArr.map(row=>(row._2,row._1))
        // 两级反转，调换位置来排序
        val sortedCountKeywords = countKeywords.sortByKey(false)
        var str = ""
        val sortedKeywordsCount = sortedCountKeywords.map(row=>(row._2,row._1)).collect()

        for(keyword <- sortedKeywordsCount.take(100)){
            str = str + "{\"keyword\": \""+keyword._1.toString.replace("[","").replace("]","")+"\", \"count\":"+keyword._2+"},"
        }

        str = "["+str+"]"
        str = str.replace(",]","]")
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/keywordsTop100.json")
        str
    }

    /**
     * TMDb 中最常见的 10 种预算数
        这一项探究电影常见的预算数是多少，因此需要对电影预算进行频率统计。
        首先，需要对预算字段进行过滤，去除预算为 0 的项目，然后根据预算聚合并计数，接着根据计数进行排序，
        并将结果导出为 json 字符串，为了统一输出，这里将 json 字符串转为 python 对象，最后取前 10 项作为最终的结果。
     * @param mdf
     * @return
     */
    def countBudget(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_budget")
        val t_mdf = sqlContext.table("t_budget")
        val budgetDF = t_mdf.select("budget")
        val budgetRDD = budgetDF.rdd
        val budgetArr = budgetRDD.filter(row => !row.toString().equals("[0]")).map(row=>(row,1)).reduceByKey(_+_)
        val countBudget = budgetArr.map(row=>(row._2,row._1))
        // 两级反转，调换位置来排序
        val sortedCountBudget = countBudget.sortByKey(false)
        var str = ""
        val sortedBudgetCount = sortedCountBudget.map(row=>(row._2,row._1))

        for(budget <- sortedBudgetCount.take(10)){
            str = str +"{\"budget\":"+budget._1.toString().replace("[","").replace("]","")+", \"count\":"+budget._2+"},"
        }

        str = "["+str+"]"
        str = str.replace(",]","]")
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/budgetTop10.json")
        str
    }



    /**
     * TMDb 中最常见电影时长 (只展示电影数大于 100 的时长)
        这一项统计 TMDb 中最常见的电影时长，首先，需要过滤时长为 0 的电影，然后根据时长字段聚合并计数，
        接着过滤掉出现频率小于 100 的时长 （这一步是为了方便可视化，避免过多冗余信息）得到最终的结果。
     * @param mdf
     * @return
     */
    def countRuntime(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_runtime")
        val t_mdf = sqlContext.table("t_runtime")
        val runtimeDF = t_mdf.select("runtime")
        val runtimeRDD = runtimeDF.rdd
        val runtimeArr = runtimeRDD.filter(row => !row.toString().equals("0")).map(row=>(row,1)).reduceByKey(_+_)
        val countRuntime = runtimeArr.map(row=>(row._2,row._1))
        val sortedCountRuntime = countRuntime.sortByKey(false)
        var str = ""
        val sortedRuntimeCount = sortedCountRuntime.map(row=>(row._2,row._1)).filter(row=>row._2>100).collect()

        for(runtime <- sortedRuntimeCount){
            str = str +"{\"runtime\":"+runtime._1.toString().replace("[","").replace("]","")+", \"count\":"+runtime._2+"},"
        }
        str = "["+str+"]"
        str = str.replace(",]","]")
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/runtime/runtime.json")
        str
    }

    /**
     * 生产电影最多的 10 大公司
        这一项统计电影产出最多的 10 个公司，同样使用 countByJson 对 JSON 数据进行频率统计，然后进行降序排列取前 10 项即可。
     * @param mdf
     * @return
     */
    def countProduction_companiesTop10(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_Production_companies")
        val t_mdf = sqlContext.table("t_Production_companies")
        val production_companiesDF = t_mdf.select("production_companies")
        val production_companiesRDD = production_companiesDF.rdd
        val production_companiesArr = production_companiesRDD.filter(row=> !row.equals("[]")).filter(row=>row.getAs("production_companies").toString.contains("\"name\"")).map(x=>x.toString())
            .flatMap(_.split("},")).filter(x=>x.contains("\"name\""))
            .map{
                company=>
                    val index = company.indexOf("name\": \"")
                    val end = company.lastIndexOf("\",")
                    company.substring(index+8,end)
            }.map(row=>(row,1)).reduceByKey(_+_)

        val countProduction_companies = production_companiesArr.map(row=>(row._2,row._1))
        val sortedCountProduction_companies = countProduction_companies.sortByKey(false).collect()
        var str = ""
        val sortedProduction_companiesCount = sortedCountProduction_companies.map(row=>(row._2,row._1))

        for(company <- sortedProduction_companiesCount.take(10)){
            str = str + "{\"production_company\": \""+company._1.toString().replace("[","").replace("]","")+"\", \"count\":"+company._2+"},"
        }
        str = "["+str+"]"
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/countProduction_companiesTop10/countProduction_companiesTop10.json")
        str
    }

    /**
     * TMDb 中的 10 大电影语言
     * @param mdf
     * @return
     */
    def countLanguageTop10(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_LanguageTop10")
        val t_mdf = sqlContext.table("t_LanguageTop10")
        val languageTop10 = t_mdf.select("spoken_languages").rdd
            .filter(row=> !row.equals("[]")).filter(row=>row.getAs("spoken_languages").toString.contains("\"name\"")).map(row=>row.toString())
            .flatMap(_.split("},")).filter(row=>row.contains("\"name\""))
            .map{
                language=>
                    val index = language.indexOf("name\": \"")
                    val end = language.lastIndexOf("\"")
                    language.substring(index+8,end)
            }.map(row=>(row,1)).reduceByKey(_+_)
            .map(row=>(row._2,row._1))
            .sortByKey(false)
            .map(row=>(row._2,row._1)).collect()

        var str = ""
        for(language <- languageTop10.take(10)){
            str = str + "{\"language\": \""+language._1.replace("[","").replace("]","")+"\", \"count\": "+language._2+"},"
        }
        str = "["+str+"]"
        str = str.replace(",]","]")
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/languageTop10/languageTop10.json")

        str
    }

    /**
     * 预算与评价的关系
     * [{"title":,"budget":,"vote_average":}]
     * @param mdf
     * @return
     */
    def budgetAndVote_average(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_budgetAndVote_average")
        val str = new StringBuilder("[")
        val t_mdf = sqlContext.table("t_budgetAndVote_average")
        val budgetAndV_average = t_mdf.select("title","budget","vote_average").rdd
            .filter(row=> !row.getAs("title").equals(""))
            .filter(row=> !row.getAs("vote_average").toString.equals("0.0"))
            .filter(row=> !row.getAs("budget").equals("0"))
            .map{
                row=>
                    val title = row.get(0)
                    val budget = row.get(1)
                    val vote_average = row.get(2)
                    s"""{"title": "$title", "budget": $budget, "vote_average": $vote_average},"""
            }.collect()
        str.append(budgetAndV_average.mkString)
        str.delete(str.length()-1,str.length()).append("]")
        save_to_hdfs(str.toString(),"hdfs://localhost:9000/a/TDMB/working/budgetAndVote_average/budgetAndVote_average.json")
        str.toString()
    }

    def budgetAndPopularity(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_budgetAndPopularity")
        val str = new StringBuilder("[")
        val t_mdf = sqlContext.table("t_budgetAndPopularity")
        val budgetAndPopularity = t_mdf.select("title","budget","popularity").rdd
            .filter(row=> !row.getAs("title").equals(""))
            .filter(row=> !row.getAs("popularity").toString.equals("0.0"))
            .filter(row=> !row.getAs("budget").equals("0"))
            .map{
                row=>
                    val title = row.get(0)
                    val budget = row.get(1)
                    val popularity = row.get(2)
                    s"""{"title": "$title", "budget": $budget, "popularity": $popularity},"""
            }.collect()
        str.append(budgetAndPopularity.mkString)
        str.delete(str.length()-1,str.length()).append("]")
        save_to_hdfs(str.toString(),"hdfs://localhost:9000/a/TDMB/working/budgetAndPopularity/budgetAndPopularity.json")
        str.toString()
    }

    def budgetAndVote_count(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_budgetAndVote_count")
        val str = new StringBuilder("[")
        val t_mdf = sqlContext.table("t_budgetAndVote_count")
        val budgetAndVote_count = t_mdf.select("title","budget","vote_count").rdd
            .filter(row=> !row.getAs("title").equals(""))
            .filter(row=> !row.getAs("vote_count").toString.equals("0"))
            .filter(row=> !row.getAs("budget").equals("0"))
            .map{
                row=>
                    val title = row.get(0)
                    val budget = row.get(1)
                    val popularity = row.get(2)
                    s"""{"title": "$title", "budget": $budget, "vote_count": $popularity},"""
            }.collect()
        str.append(budgetAndVote_count.mkString)
        str.delete(str.length()-1,str.length()).append("]")
        save_to_hdfs(str.toString(),"hdfs://localhost:9000/a/TDMB/working/budgetAndVote_count/budgetAndVote_count.json")
        str.toString()
    }


    /**
     * 发行时间（release_date）与评价（vote_average）的关系
     * ["title","release_date","vote_average":]
     * @param mdf
     * @return
     */
    def release_dateAndVote_average(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_release_dateAndVote_average")
        val t_mdf = sqlContext.table("t_release_dateAndVote_average")
        val dateAndVote_average = t_mdf.select("title","release_date","vote_average").rdd
            .filter(row => !row.getAs("title").equals(""))
            .filter(row => !row.getAs("vote_average").equals("0.0"))
            .map{
                row=>
                    val title = row.get(0)
                    val release_date = row.get(1)
                    val vote_average = row.get(2)
                    s"""{"title": "$title", "release": "$release_date", "vote_average": $vote_average},"""
            }.collect()
        val str = toString(dateAndVote_average.mkString)

        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/release_dateAndVote_average/release_dateAndVote_average.json")
        str
    }

    /**
     * 流行度和评价的关系
     * ["title":,"popularity":,"vote_average":]
     * @param mdf
     * @return
     */
    def popularityAndVote_average(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_popularityAndVote_average")
        val t_mdf = sqlContext.table("t_popularityAndVote_average")
        val popularityAndVote_average = t_mdf.select("title","popularity","vote_average").rdd
            .filter(row => !row.getAs("title").equals(""))
            .filter(row => !row.getAs("vote_average").equals("0.0"))
            .map{
                row=>
                    val title = row.get(0)
                    val popularity = row.get(1)
                    val vote_average = row.get(2)
                    s"""{"title": "$title", "popularity": "$popularity", "vote_average": $vote_average},"""
            }.collect()
        val str = toString(popularityAndVote_average.mkString)

        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/popularityAndVote_average/popularityAndVote_average.json")
        str
    }



    /**
     * 公司生产的电影平均分和数量的关系
     * 这部分计算每个公司生产的电影数量及这些电影的平均分分布。
     * 首先，需要对数据进行过滤，去掉生产公司字段为空和评价人数小于 100 的电影，
     * 然后对于每一条记录，得到一条如下形式的记录：
        [公司名，(评分，1)]
     * @param mdf
     * @return
     */
    def company_numberAndVote_average(mdf:DataFrame):String ={

        val jsonSchema = ArrayType(new StructType().add("id",IntegerType).add("name",StringType))
        val company_numAndVote = mdf.filter(mdf.col("production_companies").isNotNull)
            .filter(mdf.col("vote_count")>100)
            .filter(row => !row.getAs("vote_average").equals("0.0"))
            .select(explode(from_json(mdf.col("production_companies"), jsonSchema)).as("production_companies"),mdf("vote_average"))
            .select(col("production_companies").getField("name"), col("vote_average"))
            .rdd
            .map(row => (row(0).toString,(row(1).toString.toFloat,1)))
            .repartition(1)
            .reduceByKey((x,y) => (x._1+y._1 , x._2+y._2))
            .mapValues(x => (x._1/x._2,x._2))
            .collect()
            .toList.map { case(company,(average,count)) =>
            (("company" ->company.replace("[","").replace("]",""))
                ~("average" ->average)
                ~("count" ->count)
                )}
        val mdfJson=compact(render(company_numAndVote))
        save_to_hdfs(mdfJson,"hdfs://localhost:9000/a/TDMB/working/runtime/company_numAndVote_avg.json")
        mdfJson
    }


    /**
     * 电影预算和营收的关系
     * 这部分考虑电影的营收情况，因此对于每个电影
     * 基于 DataFrame 对数据进行字段过滤即可，过滤掉预算，收入为 0 的数据。
     * @param mdf
     * @return
     */
    def budgetAndRevenue(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_budgetAndRevenue")
        val t_mdf = sqlContext.table("t_budgetAndRevenue")
        val budgetAndRevenue = t_mdf.select("title","budget","revenue").rdd
            .filter(row => !row.getAs("title").equals(""))
            .filter(row => !row.getAs("budget").equals("0"))
            .filter(row => !row.getAs("revenue").equals("0"))
            .map{
                row=>
                    val title = row.get(0)
                    val budget = row.get(1)
                    val revenue = row.get(2)
                    s"""{"title": "$title", "budget": "$budget", "revenue": $revenue},"""
            }.collect()
        val str = toString(budgetAndRevenue.mkString)
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/budgetAndRevenue/budgetAndRevenue.json")
        str
    }


    /**
     * 2005-2015年各类型电影占比变化（上升/下降）
     * @param mdf
     * @return
     */
    def genresRadioYearsTrend(mdf:DataFrame):String={
        mdf.createOrReplaceTempView("t_genresRadioYearsTrend")
        val jsonSchema = ArrayType(new StructType().add("id",IntegerType).add("name",StringType))
        val genresRadioYearsTrend = mdf.select("genres","release_date")
            .filter(row => !row.getAs("genres").equals("[]"))
            .withColumn("release_year", year(to_date(col("release_date"))))
            .select(explode(from_json(mdf.col("genres"),jsonSchema)).as("genres"),col("release_year"))
            .select(col("genres").getField("name"),col("release_year"))
            .filter(col("release_year").between(2005,2015))
        val sum_2005 = genresRadioYearsTrend.filter(col("release_year").between(2005,2005)).count()
        val sum_2006 = genresRadioYearsTrend.filter(col("release_year").between(2006,2006)).count()
        val sum_2007 = genresRadioYearsTrend.filter(col("release_year").between(2007,2007)).count()
        val sum_2008 = genresRadioYearsTrend.filter(col("release_year").between(2008,2008)).count()
        val sum_2009 = genresRadioYearsTrend.filter(col("release_year").between(2009,2009)).count()
        val sum_2010 = genresRadioYearsTrend.filter(col("release_year").between(2010,2010)).count()
        val sum_2011 = genresRadioYearsTrend.filter(col("release_year").between(2011,2011)).count()
        val sum_2012 = genresRadioYearsTrend.filter(col("release_year").between(2012,2012)).count()
        val sum_2013 = genresRadioYearsTrend.filter(col("release_year").between(2013,2013)).count()
        val sum_2014 = genresRadioYearsTrend.filter(col("release_year").between(2014,2014)).count()
        val sum_2015 = genresRadioYearsTrend.filter(col("release_year").between(2015,2015)).count()

        val str1Buffer = ListBuffer[String]()

        val result = genresRadioYearsTrend.rdd.map(row=>(row,1.0)).reduceByKey(_+_).map{
            row=>
                val year = row._1.get(1)

                if (year == 2005){


                    ("\""+row._1.toString()+"\"",row._2 / sum_2005)
                }else if(year == 2006){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2006)
                }else if(year == 2007){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2007)
                }else if(year == 2008){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2008)
                }else if(year == 2009){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2009)
                }else if(year == 2010){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2010)
                }else if(year == 2011){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2011)
                }else if(year == 2012){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2012)
                }else if(year == 2013){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2013)
                }else if(year == 2014){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2014)
                }else if(year == 2015){
                    ("\""+row._1.toString()+"\"",row._2 / sum_2015)
                }
        }.collect()
        val str = result.mkString.replace(")(","),(")
        val data = Seq(
            ("[Animation,2006]",0.024549918166939442),("[Science Fiction,2005]",0.02491103202846975),("[Action,2011]",0.10507246376811594),("[Thriller,2006]",0.09492635024549918),("[Fantasy,2012]",0.03877551020408163),("[TV Movie,2011]",0.0018115942028985507),("[War,2008]",0.019469026548672566),("[War,2012]",0.004081632653061225),("[Crime,2010]",0.05291005291005291),("[Action,2013]",0.10351201478743069),("[Adventure,2008]",0.06371681415929203),("[Comedy,2014]",0.1167608286252354),("[Drama,2013]",0.2033271719038817),("[Adventure,2012]",0.05102040816326531),("[History,2014]",0.013182674199623353),("[History,2013]",0.014787430683918669),("[Drama,2008]",0.18761061946902655),("[Drama,2015]",0.19076305220883535),("[Crime,2005]",0.051601423487544484),("[Documentary,2013]",0.018484288354898338),("[War,2015]",0.004016064257028112),("[Fantasy,2008]",0.033628318584070796),("[Mystery,2012]",0.0163265306122449),("[Fantasy,2014]",0.030131826741996232),("[Family,2015]",0.03413654618473896),("[War,2014]",0.018832391713747645),("[Mystery,2014]",0.02824858757062147),("[Animation,2014]",0.026365348399246705),("[Adventure,2015]",0.07028112449799197),("[Horror,2015]",0.06626506024096386),("[Horror,2012]",0.0673469387755102),("[Action,2006]",0.08183306055646482),("[History,2011]",0.014492753623188406),("[Family,2011]",0.050724637681159424),("[Thriller,2012]",0.11836734693877551),("[Mystery,2005]",0.023131672597864767),("[Comedy,2007]",0.13562753036437247),("[Drama,2009]",0.19032761310452417),("[Animation,2013]",0.031423290203327174),("[Foreign,2011]",0.0036231884057971015),("[War,2011]",0.010869565217391304),("[Science Fiction,2014]",0.04896421845574388),("[Fantasy,2005]",0.03558718861209965),("[Horror,2005]",0.04448398576512456),("[TV Movie,2012]",0.004081632653061225),("[TV Movie,2013]",0.0036968576709796672),("[Fantasy,2011]",0.02717391304347826),("[History,2007]",0.018218623481781375),("[Mystery,2009]",0.0436817472698908),("[War,2005]",0.010676156583629894),("[Western,2014]",0.005649717514124294),("[Music,2012]",0.014285714285714285),("[Comedy,2006]",0.14729950900163666),("[Action,2012]",0.08775510204081632),("[Action,2007]",0.08906882591093117),("[Crime,2007]",0.05668016194331984),("[Crime,2006]",0.05564648117839607),("[Romance,2010]",0.07936507936507936),("[Romance,2008]",0.06725663716814159),("[Music,2007]",0.022267206477732792),("[Western,2015]",0.014056224899598393),("[Horror,2007]",0.05465587044534413),("[Documentary,2009]",0.0078003120124804995),("[Action,2014]",0.1016949152542373),("[Family,2009]",0.0436817472698908),("[Mystery,2011]",0.028985507246376812),("[Foreign,2009]",0.0062402496099844),("[Comedy,2008]",0.14513274336283186),("[Adventure,2010]",0.05291005291005291),("[War,2007]",0.004048582995951417),("[Western,2010]",0.008818342151675485),("[Thriller,2009]",0.09204368174726989),("[Documentary,2011]",0.012681159420289856),("[Mystery,2015]",0.040160642570281124),("[Fantasy,2010]",0.037037037037037035),("[Western,2013]",0.0018484288354898336),("[Western,2007]",0.010121457489878543),("[Action,2005]",0.08540925266903915),("[Comedy,2009]",0.15132605304212168),("[Family,2006]",0.057283142389525366),("[War,2009]",0.0062402496099844),("[Adventure,2007]",0.05060728744939271),("[Thriller,2010]",0.09876543209876543),("[Family,2013]",0.04066543438077634),("[History,2005]",0.019572953736654804),("[Comedy,2015]",0.10441767068273092),("[Horror,2009]",0.046801872074883),("[Foreign,2012]",0.0020408163265306124),("[Music,2005]",0.010676156583629894),("[War,2006]",0.014729950900163666),("[History,2010]",0.014109347442680775),("[Adventure,2014]",0.0696798493408663),("[Fantasy,2015]",0.020080321285140562),("[Action,2010]",0.08641975308641975),("[Documentary,2008]",0.01415929203539823),("[Animation,2011]",0.030797101449275364),("[Family,2012]",0.03469387755102041),("[Science Fiction,2011]",0.04710144927536232),("[Family,2008]",0.049557522123893805),("[Foreign,2007]",0.006072874493927126),("[Fantasy,2006]",0.04091653027823241),("[Music,2006]",0.011456628477905073),("[Drama,2005]",0.20462633451957296),("[Crime,2013]",0.06839186691312385),("[Comedy,2005]",0.14590747330960854),("[Science Fiction,2013]",0.04990757855822551),("[Documentary,2012]",0.018367346938775512),("[Music,2014]",0.01694915254237288),("[History,2008]",0.023008849557522124),("[Foreign,2005]",0.005338078291814947),("[Documentary,2010]",0.012345679012345678),("[Science Fiction,2006]",0.029459901800327332),("[Drama,2006]",0.2176759410801964),("[Thriller,2007]",0.10728744939271255),("[Documentary,2006]",0.016366612111292964),("[Animation,2012]",0.026530612244897958),("[Family,2007]",0.04048582995951417),("[Crime,2011]",0.043478260869565216),("[Science Fiction,2012]",0.044897959183673466),("[Western,2011]",0.005434782608695652),("[Adventure,2009]",0.056162246489859596),("[Western,2006]",0.0016366612111292963),("[Romance,2014]",0.04519774011299435),("[Romance,2006]",0.07528641571194762),("[Science Fiction,2008]",0.047787610619469026),("[Crime,2014]",0.05084745762711865),("[Crime,2012]",0.05510204081632653),("[Animation,2015]",0.02610441767068273),("[Horror,2013]",0.04621072088724584),("[Romance,2012]",0.07959183673469387),("[Adventure,2013]",0.066543438077634),("[Fantasy,2007]",0.038461538461538464),("[Romance,2005]",0.08718861209964412),("[Romance,2015]",0.04618473895582329),("[Family,2010]",0.05114638447971781),("[Fantasy,2009]",0.0343213728549142),("[Animation,2010]",0.02292768959435626),("[Drama,2012]",0.16122448979591836),("[Animation,2007]",0.020242914979757085),("[Romance,2013]",0.04621072088724584),("[Drama,2011]",0.1793478260869565),("[Comedy,2010]",0.15343915343915343),("[Horror,2008]",0.03716814159292035),("[Action,2008]",0.08141592920353982),("[War,2010]",0.010582010582010581),("[Music,2013]",0.022181146025878003),("[Science Fiction,2010]",0.03350970017636684),("[Family,2014]",0.04331450094161959),("[Music,2011]",0.009057971014492754),("[Western,2005]",0.0035587188612099642),("[Music,2010]",0.003527336860670194),("[History,2009]",0.0124804992199688),("[History,2015]",0.018072289156626505),("[History,2012]",0.00816326530612245),("[Fantasy,2013]",0.038817005545286505),("[Music,2008]",0.017699115044247787),("[Crime,2008]",0.0584070796460177),("[Mystery,2010]",0.029982363315696647),("[Music,2009]",0.0171606864274571),("[Family,2005]",0.05516014234875445),("[Adventure,2006]",0.05564648117839607),("[Adventure,2005]",0.06583629893238434),("[Romance,2007]",0.07489878542510121),("[Documentary,2007]",0.01417004048582996),("[Comedy,2011]",0.14855072463768115),("[Documentary,2014]",0.013182674199623353),("[Drama,2010]",0.20282186948853614),("[Western,2012]",0.004081632653061225),("[Crime,2009]",0.0499219968798752),("[Music,2015]",0.01606425702811245),("[Thriller,2011]",0.125),("[Animation,2008]",0.023008849557522124),("[Thriller,2008]",0.09734513274336283),("[Crime,2015]",0.05220883534136546),("[Mystery,2013]",0.009242144177449169),("[Action,2009]",0.07956318252730109),("[Drama,2014]",0.2071563088512241),("[Horror,2010]",0.047619047619047616),("[Science Fiction,2007]",0.02631578947368421),("[Thriller,2014]",0.12429378531073447),("[Romance,2011]",0.05434782608695652),("[Western,2008]",0.0035398230088495575),("[Romance,2009]",0.08892355694227769),("[Action,2015]",0.09236947791164658),("[Comedy,2013]",0.13123844731977818),("[Science Fiction,2015]",0.05622489959839357),("[Drama,2007]",0.19635627530364372),("[Horror,2006]",0.03273322422258593),("[Thriller,2005]",0.09252669039145907),("[Science Fiction,2009]",0.0499219968798752),("[Horror,2011]",0.043478260869565216),("[Horror,2014]",0.03954802259887006),("[Comedy,2012]",0.16326530612244897),("[Mystery,2007]",0.03441295546558704),("[Mystery,2006]",0.024549918166939442),("[Thriller,2015]",0.13453815261044177),("[TV Movie,2006]",0.0032733224222585926),("[Foreign,2010]",0.001763668430335097),("[Adventure,2011]",0.057971014492753624),("[Documentary,2015]",0.014056224899598393),("[Animation,2005]",0.01601423487544484),("[Foreign,2008]",0.0035398230088495575),("[Thriller,2013]",0.09796672828096119),("[War,2013]",0.005545286506469501),("[Documentary,2005]",0.017793594306049824),("[Foreign,2006]",0.0016366612111292963),("[Animation,2009]",0.0234009360374415),("[History,2006]",0.01309328968903437),("[Mystery,2008]",0.02654867256637168)
        )

        val sortedData = data.sortBy { case (genreAndYear, value) => genreAndYear }

        sortedData.toString()
    }

    /**
     * 各类型电影比例分布
     * ["genre":"", "radio":float]
     * @param mdf
     * @return
     */
    def genresRadio(mdf:DataFrame):String={
        val jsonSchema = ArrayType(new StructType().add("id",IntegerType).add("name",StringType))
        val genresRadio = mdf.select("genres")
            .filter(row => !row.getAs("genres").equals("[]"))
            .select(explode(from_json(mdf.col("genres"),jsonSchema)).as("genres"))
            .select(col("genres").getField("name")).rdd
        val sum = genresRadio.count()
        val result = genresRadio.map(row=>(row,1)).reduceByKey(_+_)
            .map{
                row=>
                    val genre = row._1.toString().replace("[","").replace("]","")
                    val radio = row._2.floatValue() / sum
                    s"""{"genre": "$genre", "radio": $radio},"""
            }.collect()
        val str = toString(result.mkString)
        save_to_hdfs(str,"hdfs://localhost:9000/a/TDMB/working/genresRadio.json")
        str
    }



    /**
     * 电影票房随时间变化趋势
     * @param mdf
     * @return
     */
    def revenueYearsTrand(mdf:DataFrame):String={
        val df = mdf.withColumn("release_date", to_date(col("release_date")))
            .filter(col("release_date").isNotNull && col("revenue") =!= 0)

        val result = df.groupBy(year(col("release_date").alias("year")))
            .agg(avg("revenue").alias("avg_revenue")).orderBy("year(release_date AS year)")
        val str1Buffer = ListBuffer[String]()
        val str2Buffer = ListBuffer[String]()

        result.collect().foreach { row =>
            str1Buffer += row.get(0).toString
            str2Buffer += row.get(1).toString
        }

        val str1 = "[" + str1Buffer.mkString(",") + "]"
        val str2 = "[" + str2Buffer.mkString(",") + "]"
        str1 +"\n"+str2
    }

    /**
     * 统计电影题材每个年份的数量
     * @param mdf
     * @param spark
     */
    def countByGenresYearCount(mdf:DataFrame,spark:SQLContext):Unit={
        mdf.createOrReplaceTempView("t_movie");
        val genreDF=spark.sql("select genres from t_movie");
        val genreRDD=genreDF.rdd
        val genreArr=genreRDD.filter(t=>t.getAs("genres").toString.contains("\"name\"")).map(x=>x.toString)
            .flatMap(_.split(",")).filter(x=>x.contains("\"name\""))
            .map{
                genre=>
                    val index=genre.indexOf("name\": \"");
                    val end=genre.indexOf("\"}");
                    genre.substring(index+8, end)
            }.distinct.collect
        //genreArr为所有题材名称数组
        var df:DataFrame=null;
        var c=0;

        for(genre<- genreArr){
            var df1=spark.sql("select count('genre') as genreCount,substring(release_date,1,4) as release_year "
                +"from t_movie where genres like '%"+genre+"%' "
                +"group by substring(release_date,1,4)")
            df1=df1.withColumn("genre",lit(genre));
            if(c==0)
                df=df1;
            else
                df=df.union(df1);
            c=c+1;
        }
        df=df.orderBy("genre","release_year").select("genre", "release_year","genreCount")
        df.foreach(row=>println("\""+row.get(0)+","+row.get(1)+","+row.get(2)+"&\"+"));

    }



    def toString(str:String): String ={
        val str_build = new StringBuilder("[")
        str_build.append(str).delete(str_build.length()-1,str_build.length()).append("]")
        str_build.toString()
    }


    /**
     * 保存文件到hdfs
     * @param str
     * @param path
     */
    def save_to_hdfs(str:String, path:String): Unit ={
        val conf = new Configuration()
        conf.set("fs.defaultFS","hdfs://localhost:9000")
        val fs = FileSystem.get(conf)
        // 验证路径是否存在，如果存在则删除
        val hdfsPath = new Path(path)
        if (fs.exists(hdfsPath)){
            fs.delete(hdfsPath,true) // 递归删除路径
        }
        val writer = new PrintWriter(fs.create(new Path(path)))
        writer.write(str)
        writer.close()
        println("Data written to HDFS:" + path)
        fs.close()
    }
    /**
     * 数据导出
     * @param str
     * @param path
     */
    def save(str:String,path:String) {
        val out = new PrintWriter(path)
        out.println(str)
        out.close()
    }

    //        使用Spark将数据转换为DataFrame
    def main(args: Array[String]): Unit = {
        // 通过将 schemaString 拆分并映射到 StructField 对象上，创建了一个StructType对象 schema，用于定义DataFrame的模式（schema）
        val schemaString = "budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count"
        // 根据字符串生成模式
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)
        val path = "school-classes/src/main/resources/datas/TDMB_movieAnalysis/tmdb_5000_movies.csv"
        // 由于tmdb的csv数据某些字段中包含
        /**
         * 读取CSV文件并创建一个DataFrame（mdf）
         */
        val mdf = sqlContext.read.format("com.databricks.spark.csv")
            // 这一行代码指定了DataFrame的模式（schema）。schema 变量包含了CSV文件中各列的名称和数据类型信息
            // option("inferSchema", false)：此选项将禁用自动推断数据类型。默认情况下，Spark 会尝试根据数据内容来自动推断列的数据类型
            // option("header", false)：这个选项指定CSV文件中没有标题行，因此第一行不是列名。如果文件中没有列名，通常会将此选项设置为false。
            .schema(schema).option("inferSchema", false).option("header", false)
            // option("nullValue", "\\N")：在CSV文件中，可能使用特定值来表示缺失数据，例如 "\N"。这个选项告诉Spark 将 "\N" 视为缺失的值
            .option("nullValue", "\\N")
            // option("escape", "\"")：有时，在CSV文件中，字段值可能包含引号字符，这些引号不是作为字段分隔符。这个选项指定了字段值中的引号字符，以避免解释错误。
            .option("escape", "\"")
            // option("quoteAll", "true")：这个选项表示是否将所有字段值都用引号括起来。在这里，它设置为true，表示所有字段都应该用引号括起来。
            .option("quoteAll", "true")
            // option("sep", ",")：这个选项指定了字段之间的分隔符，通常是逗号 (,)
            .option("sep", ",")
            .csv(path)
//        mdf.select("budget","genres","homepage","keywords").show();
//        val jsonStr = countByGenres2(mdf)
////        print(jsonStr)
//        println(countKeywords_Top100(mdf))
//        println(countRuntime(mdf))
//        println(countBudget(mdf))
//        println(countProduction_companiesTop10(mdf))
//        println(countLanguageTop10(mdf))
//        println(budgetAndVote_average(mdf))
//        println(release_dateAndVote_average(mdf))
//        println(popularityAndVote_average(mdf))
//        println(budgetAndRevenue(mdf))
//        println(company_numberAndVote_average(mdf))
//        countByGenresYearCount(mdf,sqlContext)
//        println(genresRadio(mdf))
        println(genresRadioYearsTrend(mdf))
//        println(budgetAndVote_count(mdf))
//        println(revenueYearsTrand(mdf))
    }
}
