package it.polimi.middleware.spark.project5;

import it.polimi.middleware.spark.utils.LogUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * Project5: Analysis of COVID-19 Data
 *
 * Input: csv files with report of Covid19, having the following
 * schema (dateRep: String, day: Int, month: Int ,year: Int, cases: Int, deaths:Int, countriesAndTerritories: String, geoId: String, countryTerritoryCode: String, popData2019: Int, continentExp: String, Cumulative_number_for_14_days_of_COVID_19_cases_per_100000: Float)
 *
 * Queries
 * Q1. Seven days moving average of new reported cases, for each country and for each day.
 * Q2. Percentage increase (with respect to the day before) of the seven days moving average, for each country and for each day.
 * Q3. Top 10 countries with the highest percentage increase of the seven days moving average, for each day.
 */

public class Covid19 {
    private static final boolean useCache = true;
    private static final boolean top10WithEq = false;

    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[6]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "Covid19WithCache" : "Covid19NoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("dateRep", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("day", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("month", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("cases", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("deaths", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("countriesAndTerritories", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("geoId", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("countryTerritoryCode", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("popData2019", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("continentExp", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("Cumulative_number_for_14_days_of_COVID_19_cases_per_100000", DataTypes.FloatType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        final Dataset<Row> source = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/covid19/COVID-19-geographic-distribution-worldwide-2020-12-14.csv");


        if (useCache) {
            source.cache();
        }
        source.show(100,false);

        //Substitution of dateRep to obtain the format yyyy-MM-dd
        final Dataset<Row> source1 = source.withColumn("dateRep", concat(col("year"), lit('-'),  col("month"), lit('-'),col("day")));
        if (useCache) {
            source1.cache();
        }
        //source1.show();


        //Casting from StringType to DateType.
        final Dataset<Row> source2 = source1.withColumn("dateRep", to_date(col("dateRep")));
        if (useCache) {
            source2.cache();
        }
        //source2.show(100,false);

        source2.createOrReplaceTempView("source2_sql");

        final Dataset<Row> q1 = spark
                .sql("SELECT x.dateRep, x.day, x.month, x.year, x.cases, x.deaths,x.countriesAndTerritories, x.geoId, x.countryTerritoryCode, x.popData2019, x.continentExp, x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000, avg(y.cases) as last_7_days_avg " + //other option: sum(y.cases)/7. It operate even if some of the seven days before are not present.
                " FROM source2_sql as x, source2_sql as y" +
                " WHERE y.dateRep between " + date_sub(col("x.dateRep"), 6) + " and " + col("x.dateRep") +
                " and x.countriesAndTerritories = y.countriesAndTerritories" +
                " GROUP BY x.dateRep, x.day, x.month, x.year, x.cases, x.deaths,x.countriesAndTerritories, x.geoId, x.countryTerritoryCode, x.popData2019, x.continentExp, x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000 " +
                " ORDER BY x.dateRep desc, x.countriesAndTerritories");

        //To run this version, please comment the previous 2 instructions.
        /*final Dataset<Row> q1 = source2.as("x")
                .join(source2.as("y"), col("x.countriesAndTerritories").equalTo(col("y.countriesAndTerritories")),"left_outer")
                .filter(col("y.dateRep").between(date_sub(col("x.dateRep"), 6), col("x.dateRep")))
                .groupBy(col("x.dateRep"),col("x.day"),col("x.month"), col("x.year"), col("x.cases"), col("x.deaths"), col("x.countriesAndTerritories"), col("x.geoId"), col("x.countryTerritoryCode"), col("x.popData2019"), col("x.continentExp"), col("x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000"))
                .agg(avg("y.cases"))
                .withColumnRenamed("avg(y.cases)","last_7_days_avg")
                .orderBy(desc("x.dateRep"), asc("x.countriesAndTerritories"));*/

        if (useCache) {
            q1.cache();
        }
        q1.show(100,false);
        if(useCache) {
            source.unpersist();
            source1.unpersist();
            source2.unpersist();
        }

        q1.createOrReplaceTempView("q1_sql");
        final Dataset<Row> q2 = spark
                .sql("SELECT x.dateRep, x.day, x.month, x.year, x.cases, x.deaths,x.countriesAndTerritories, x.geoId, x.countryTerritoryCode, x.popData2019, x.continentExp, x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000, x.last_7_days_avg, ((x.last_7_days_avg - y.last_7_days_avg)/y.last_7_days_avg)*100 as PercentageIncreaseWrtDayBefore " +
                " FROM q1_sql as x, q1_sql as y" +
                " WHERE y.dateRep =" + date_sub(col("x.dateRep"), 1) +
                " and x.countriesAndTerritories = y.countriesAndTerritories" +
                " ORDER BY x.dateRep desc, x.countriesAndTerritories");

        //To run this version, please comment the previous 2 instructions.
        /*final Dataset<Row> q2 = q1.as("x")
                .join(q1.as("y"), col("x.countriesAndTerritories").equalTo(col("y.countriesAndTerritories")),"left_outer")
                .filter(col("y.dateRep").equalTo(date_sub(col("x.dateRep"), 1)))
                .select(col("x.dateRep"),col("x.day"),col("x.month"), col("x.year"), col("x.cases"), col("x.deaths"), col("x.countriesAndTerritories"), col("x.geoId"), col("x.countryTerritoryCode"), col("x.popData2019"), col("x.continentExp"), col("x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000"), col("x.last_7_days_avg"), (col("x.last_7_days_avg").minus(col("y.last_7_days_avg")).divide(col("y.last_7_days_avg")).multiply(100)))
                .withColumnRenamed("(((x.last_7_days_avg - y.last_7_days_avg) / y.last_7_days_avg) * 100)","PercentageIncreaseWrtDayBefore")
                .orderBy(desc("x.dateRep"), asc("x.countriesAndTerritories"));*/

        if (useCache) {
            q2.cache();
        }
        q2.show(100,false);
        if(useCache) {
            q1.unpersist();
        }

        String temp = "";
        if(!top10WithEq) temp = "or (x.PercentageIncreaseWrtDayBefore=y.PercentageIncreaseWrtDayBefore and x.countriesAndTerritories>y.countriesAndTerritories)";


        q2.createOrReplaceTempView("q2_sql");
        final Dataset<Row> preQ3 = spark
                .sql("SELECT x.dateRep, x.day, x.month, x.year, x.cases, x.deaths, x.countriesAndTerritories, x.geoId, x.countryTerritoryCode, x.popData2019, x.continentExp, x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000, x.last_7_days_avg, x.PercentageIncreaseWrtDayBefore, count(y.countriesAndTerritories)-1 as countriesWithGreaterPercentageIncreaseWrtDayBefore" +
                " FROM q2_sql as x, q2_sql as y" +
                " WHERE x.dateRep=y.dateRep and x.countriesAndTerritories!=y.countriesAndTerritories and (x.PercentageIncreaseWrtDayBefore<y.PercentageIncreaseWrtDayBefore " + temp + ")"  +
                " GROUP BY x.dateRep, x.countriesAndTerritories, x.PercentageIncreaseWrtDayBefore, x.day, x.month, x.year, x.cases, x.deaths, x.geoId, x.countryTerritoryCode, x.popData2019, x.continentExp, x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000, x.last_7_days_avg " +
                " ORDER BY x.dateRep desc, x.countriesAndTerritories");

        if (useCache) {
            preQ3.cache();
        }
        //preQ3.show(100,false);

        //To run this version, please comment the previous 3 instructions and the next instruction.
        /*final Dataset<Row> q3 = q2.as("x")
                .join(q2.as("y"), col("x.dateRep").equalTo(col("y.dateRep")),"left_outer")
                .filter((col("y.countriesAndTerritories").notEqual("x.countriesAndTerritories")).and((col("x.PercentageIncreaseWrtDayBefore").lt(col("y.PercentageIncreaseWrtDayBefore"))).or((col("x.PercentageIncreaseWrtDayBefore").equalTo("y.PercentageIncreaseWrtDayBefore")).and(col("x.countriesAndTerritories").gt(col("y.countriesAndTerritories"))))))
                .groupBy(col("x.dateRep"),col("x.day"),col("x.month"), col("x.year"), col("x.cases"), col("x.deaths"), col("x.countriesAndTerritories"), col("x.geoId"), col("x.countryTerritoryCode"), col("x.popData2019"), col("x.continentExp"), col("x.Cumulative_number_for_14_days_of_COVID_19_cases_per_100000"), col("x.last_7_days_avg"), col("x.PercentageIncreaseWrtDayBefore"))
                .agg(count("y.countriesAndTerritories").minus(1))
                .withColumnRenamed("(count(y.countriesAndTerritories) - 1)","countriesWithGreaterPercentageIncreaseWrtDayBefore")
                .orderBy(desc("x.dateRep"), asc("x.countriesAndTerritories"))
                .filter(col("countriesWithGreaterPercentageIncreaseWrtDayBefore").lt(10))
                .orderBy(desc("dateRep"),asc("countriesWithGreaterPercentageIncreaseWrtDayBefore"))
                .drop("countriesWithGreaterPercentageIncreaseWrtDayBefore");*/

        final Dataset<Row> q3 = preQ3.filter(col("countriesWithGreaterPercentageIncreaseWrtDayBefore").lt(10)).orderBy(desc("dateRep"),asc("countriesWithGreaterPercentageIncreaseWrtDayBefore")).drop("countriesWithGreaterPercentageIncreaseWrtDayBefore");
        q3.show(100,false);

        //Two options to write on file the results (for both, note that the line to write Q1 must be located in the code to exploit the caching feature, thus before the unpersist!)
        //Option 1
        //q1.toJavaRDD().map(x -> x.toString()).saveAsTextFile(filePath + "files/covid19/resultQ1");
        //q2.toJavaRDD().map(x -> x.toString()).saveAsTextFile(filePath + "files/covid19/resultQ2");
        //q3.toJavaRDD().map(x -> x.toString()).saveAsTextFile(filePath + "files/covid19/resultQ3");

        //Option 2, note: coalesce(1) slow down the performance!
        //q1.toDF("dateRep", "day" , "month" ,"year", "cases", "deaths", "countriesAndTerritories", "geoId", "countryTerritoryCode", "popData2019", "continentExp", "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000", "last_7_days_avg").coalesce(1).write().format("csv").save(filePath + "files/covid19/resultQ1CSV");
        //q2.toDF("dateRep", "day" , "month" ,"year", "cases", "deaths", "countriesAndTerritories", "geoId", "countryTerritoryCode", "popData2019", "continentExp", "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000", "last_7_days_avg", "PercentageIncreaseWrtDayBefore").coalesce(1).write().format("csv").save( filePath + "files/covid19/resultQ2CSV");
        //q3.toDF("dateRep", "day" , "month" ,"year", "cases", "deaths", "countriesAndTerritories", "geoId", "countryTerritoryCode", "popData2019", "continentExp", "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000", "last_7_days_avg", "PercentageIncreaseWrtDayBefore").coalesce(1).write().format("csv").save(filePath + "files/covid19/resultQ3CSV");

        //Uncomment to make the program wait to be closed.
        /*System.out.println("Press Enter to close...");
        Scanner myObj = new Scanner(System.in);
        myObj.nextLine();
        myObj.close();*/

        spark.close();
    }
}