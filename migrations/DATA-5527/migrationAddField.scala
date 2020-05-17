import scala.util.matching.Regex
import org.apache.hadoop.fs._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.io.Source
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

val args = spark.sqlContext.getConf("spark.driver.args").split(",")

val rootSourcePath = new Path(args(0))
val rootTmpPath = new Path(args(1))
val rootDestinationPath = new Path(args(2))

val deleteSource = args(3).toBoolean

val startdate = LocalDate.parse(args(4))
val enddate = LocalDate.parse(args(5))

val fs = FileSystem.get(sc.hadoopConfiguration)
val utils = new Utils(fs)

val partitionList = utils.getPartitionList(rootSourcePath)

if (partitionList.isEmpty) {
    utils.logInfo("No partitions suitable for the time conditions")
    System.exit(0)
}

def migrate(df: DataFrame, schema: StructType): DataFrame = {
    df.withColumn("companycountrycombination__c", lit(null).cast(StringType))
        .select(schema.fields.map(_.name).map(col): _*)
}

for (i <- partitionList) {
    utils.logInfo(s"Do partition: $i")

    // Prepare temporary location
    utils.prepateTmpLocation(rootTmpPath, i)

    // Read data
    utils.logInfo("Processing data")
    val partitionPath = utils.getPartitionPath(rootSourcePath, i)
    val df = spark.read.parquet(partitionPath.toString)
    val schema = StructType(List(
        StructField("no_of_international_locations__c", DoubleType, true),
        StructField("company_name_du__c", StringType, true),
        StructField("ah_website__c", StringType, true),
        StructField("street__c", StringType, true),
        StructField("ruid_reference_gu__c", StringType, true),
        StructField("net_multiplier__c", StringType, true),
        StructField("parent_company_d_u_n_s_number__c", StringType, true),
        StructField("currencyisocode", StringType, true),
        StructField("tech_stack_details__c", StringType, true),
        StructField("effective_no_of_employees_range__c", StringType, true),
        StructField("gu_state__c", StringType, true),
        StructField("company_name_gu__c", StringType, true),
        StructField("gu_employee_exact__c", DoubleType, true),
        StructField("target_account__c", BooleanType, true),
        StructField("industry__c", StringType, true),
        StructField("gu_phone__c", StringType, true),
        StructField("technology_stack_telco__c", StringType, true),
        StructField("gu_zipcode__c", StringType, true),
        StructField("franchise__c", BooleanType, true),
        StructField("tech_stack_categories__c", StringType, true),
        StructField("gu_effective_employees__c", StringType, true),
        StructField("ah_owner__c", StringType, true),
        StructField("industry_description__c", StringType, true),
        StructField("no_of_dfr_engagements__c", DoubleType, true),
        StructField("no_of_branches__c", DoubleType, true),
        StructField("franchise_msa__c", BooleanType, true),
        StructField("domestic_ultimate_d_u_n_s_number__c", StringType, true),
        StructField("name", StringType, true),
        StructField("sled_key__c", StringType, true),
        StructField("companycountrycombination__c", StringType, true),
        StructField("country__c", StringType, true),
        StructField("ah_phone__c", StringType, true),
        StructField("ah_family_members__c", StringType, true),
        StructField("ah_alternate_owner__c", StringType, true),
        StructField("du_alexa_rank__c", DoubleType, true),
        StructField("gu_naics__c", StringType, true),
        StructField("ownerid", StringType, true),
        StructField("company_grade__c", StringType, true),
        StructField("isdeleted", BooleanType, true),
        StructField("systemmodstamp", TimestampType, true),
        StructField("lastmodifiedbyid", StringType, true),
        StructField("partner_integration_s__c", StringType, true),
        StructField("state__c", StringType, true),
        StructField("company_score__c", DoubleType, true),
        StructField("longitude__c", DoubleType, true),
        StructField("ruid_primary_du__c", StringType, true),
        StructField("zipcode__c", StringType, true),
        StructField("no_of_subsidaries__c", DoubleType, true),
        StructField("employee_exact__c", LongType, true),
        StructField("subsidiary__c", BooleanType, true),
        StructField("sic__c", StringType, true),
        StructField("createddate", TimestampType, true),
        StructField("naic__c", StringType, true),
        StructField("latitude__c", DoubleType, true),
        StructField("d_u_n_s_number__c", StringType, true),
        StructField("technology_stack_cloud__c", StringType, true),
        StructField("gu_revenue__c", DoubleType, true),
        StructField("city__c", StringType, true),
        StructField("sic_description__c", StringType, true),
        StructField("ae_owned__c", BooleanType, true),
        StructField("gu_website__c", StringType, true),
        StructField("createdbyid", StringType, true),
        StructField("ah_territory_name__c", StringType, true),
        StructField("change_review__c", BooleanType, true),
        StructField("location_type__c", StringType, true),
        StructField("d_b_revenue__c", DoubleType, true),
        StructField("seat_potential__c", DoubleType, true),
        StructField("sfid", StringType, true),
        StructField("id", LongType, true),
        StructField("_hc_lastop", StringType, true),
        StructField("_hc_err", StringType, true),
        StructField("lastmodifieddate", TimestampType, true),
        StructField("company_status__c", StringType, true),
        StructField("technology_stack_collaboration__c", StringType, true),
        StructField("ls_ltv_predictive_score__c", DoubleType, true),
        StructField("lastvieweddate", TimestampType, true),
        StructField("ah_total_mrr__c", DoubleType, true),
        StructField("ah_total_dls__c", DoubleType, true),
        StructField("total_contact_center_users__c", DoubleType, true),
        StructField("rc_relationship_type__c", StringType, true),
        StructField("signal_type__c", StringType, true),
        StructField("lastactivitydate", TimestampType, true),
        StructField("total_glip_users__c", DoubleType, true),
        StructField("top_5__c", BooleanType, true),
        StructField("total_professional_users__c", DoubleType, true),
        StructField("total_fax_users__c", DoubleType, true),
        StructField("county__c", StringType, true),
        StructField("technology_stack_conference__c", StringType, true),
        StructField("lastreferenceddate", TimestampType, true),
        StructField("total_global_office_users__c", DoubleType, true),
        StructField("county_msa__c", StringType, true),
        StructField("gu_country__c", StringType, true),
        StructField("signal_date__c", TimestampType, true),
        StructField("direct_mgr__c", StringType, true),
        StructField("svp_wws_directs_rank__c", DoubleType, true),
        StructField("top_target__c", StringType, true),
        StructField("segment_leader_rank__c", DoubleType, true),
        StructField("segment_leader__c", StringType, true),
        StructField("direct_manager_rank__c", DoubleType, true),
        StructField("svp_wws_directs__c", StringType, true),
        StructField("coaching__c", StringType, true),
        StructField("ae_rank__c", DoubleType, true),
        StructField("ae__c", StringType, true),
        StructField("intent_bombora__c", StringType, true),
        StructField("protected_owner__c", StringType, true),
        StructField("protection_reason__c", StringType, true),
        StructField("technology_stack_contact_center__c", StringType, true),
        StructField("technology_stack_strategic_alliance__c", StringType, true),
        StructField("company_segment__c", StringType, true),
        StructField("company_domain_type__c", StringType, true),
        StructField("abm_engagement_score__c", DoubleType, true),
        StructField("abm_engagement_classification__c", StringType, true),
        StructField("isr_owner__c", StringType, true),
        StructField("bdr_owner__c", StringType, true),
        StructField("protected_bdr_owner__c", StringType, true),
        StructField("exact_estimated__c", BooleanType, true),
        StructField("surge__c", BooleanType, true),
        StructField("surge_date__c", TimestampType, true),
        StructField("bdr_top_50__c", BooleanType, true),
        StructField("parent_company_name__c", StringType, true),
        StructField("decision_making__c", StringType, true),
        StructField("partner_name__c", StringType, true),
        StructField("sum_of_contact_center_mrr__c", DoubleType, true),
        StructField("sum_of_service_mrr__c", DoubleType, true),
        StructField("office_mrr__c", DoubleType, true),
        StructField("professional_mrr__c", DoubleType, true),
        StructField("fax_mrr__c", DoubleType, true),
        StructField("rcmeetings_mrr__c", DoubleType, true),
        StructField("company_type__c", StringType, true),
        StructField("ls_normalized_predictive_score__c", DoubleType, true),
        StructField("ls_company_grade__c", StringType, true),
        StructField("dma__c", StringType, true),
        StructField("ls_global_predictive_score__c", DoubleType, true),
        StructField("sub_industry_2__c", StringType, true),
        StructField("nano_industry__c", StringType, true),
        StructField("ideal_customer_profile_contact_center__c", BooleanType, true),
        StructField("business_type__c", StringType, true),
        StructField("ideal_customer_profile_office__c", BooleanType, true),
        StructField("classification_update__c", TimestampType, true),
        StructField("marketability__c", StringType, true)
    ))

    //Migrate data
    val migratedDf = migrate(df, schema)

    //Write data to temporary location
    utils.writeDfToTmp(migratedDf, rootTmpPath, i)

    if (deleteSource) {
        // Delete source location
        utils.deleteSourceData(partitionPath, i)

        // Rename tmp to source back
        val partitionDestinationPath = utils.getPartitionPath(rootDestinationPath, i)
        utils.moveData(rootTmpPath, partitionDestinationPath, i)
    }
}

utils.logInfo("Finished")

System.exit(0)

class Utils(fs: FileSystem) {

    private val pattern = new Regex("(?!dt=)(\\d+-\\d+-\\d+)")

    def getPartitionList(sourcePath: Path): List[LocalDate] = {
        fs.listStatus(sourcePath)
            .toList
            .map(_.getPath.toString)
            .map(item => pattern findFirstIn item)
            .map(_.get)
            .map(LocalDate.parse(_))
            .filter(_.isAfter(startdate))
            .filter(_.isBefore(enddate))
    }

    def writeDfToTmp(df: DataFrame, rootTmpPath: Path, date: LocalDate) = {
        val partitionTmpPathStr = getPartitionPathStr(rootTmpPath, date)
        if (!df.rdd.isEmpty) {
            df.coalesce(20)
                .write
                .mode(SaveMode.Overwrite)
                .parquet(partitionTmpPathStr)
        }
    }

    def prepateTmpLocation(rootTmpPath: Path, date: LocalDate) = {
        val partitionTmpPath = getPartitionPath(rootTmpPath, date)
        if (fs.exists(partitionTmpPath)) {
            logInfo(s"Temporary directory $partitionTmpPath exists, deleting")
            if (fs.delete(partitionTmpPath, true)) {
                logInfo(s"Temporary location $partitionTmpPath was successfully deleted")
            } else {
                throw new Exception(s"Didn`t delete tmp partition $date")
            }
        }
        println(s"${LocalDateTime.now.toString} | Create temporary directory $partitionTmpPath")
        fs.mkdirs(partitionTmpPath)
    }

    def deleteSourceData(path: Path, date: LocalDate) = {
        logInfo("Deleting source location")
        if (fs.delete(path, true)) {
            logInfo(s"Source location $path was successfully deleted")
        } else {
            throw new Exception(s"Didn`t delete partition $date")
        }
    }

    def moveData(rootTmpPath: Path, partitionPath: Path, date: LocalDate) = {
        val partitionTmpPath = getPartitionPath(rootTmpPath, date)
        logInfo("Moving data")
        if (fs.rename(partitionTmpPath, partitionPath)) {
            logInfo(s"Partition $date was successfully copied to from tmp to target location")
        } else {
            throw new Exception(s"Error copying partition from temp location $partitionTmpPath to target $partitionPath")
        }
    }

    def getPartitionPath(path: Path, date: LocalDate): Path = {
        new Path(getPartitionPathStr(path, date))
    }

    private def getPartitionPathStr(path: Path, date: LocalDate): String = {
        s"${path.toString}/dt=$date"
    }

    def logInfo(msg: String): Unit = {
        println(s"${LocalDateTime.now.toString} | $msg")
    }
}