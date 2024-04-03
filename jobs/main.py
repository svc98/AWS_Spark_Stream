from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

from jobs.config.config import configuration
# from jobs.udf_utils import *
import re
from datetime import datetime
from word2number import w2n


def extract_file_name(file_content):
    try:
        file_content = file_content.strip()
        position = file_content.split('\n')[0]
        return position
    except Exception as e:
        raise ValueError(f"Error extracting file name: {e}")
def extract_position(file_content):
    try:
        position_match = re.search(r"([Rr]ole [Tt]itle:)\s*(\w+\s?\w*)", file_content)
        position = position_match.group(2) if position_match else None
        return position
    except Exception as e:
        raise ValueError(f"Error extracting position: {e}")

def extract_job_code(file_content):
    try:
        classcode_match = re.search(
            r"JOB CODE:\s*(\d+)",
            file_content,
            re.IGNORECASE)
        classcode = classcode_match.group(1) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f"Error extracting job code: {e}")

def extract_salary(file_content):
    try:
        salary_pattern = re.search(
            r"\$(\d{1,3},\d{3})\s+(to|-)\s+\$(\d{1,3},\d{3});\s?\$?(\d{1,3},\d{3})?\s?(to|-)?\s?\$?(\d{1,3},\d{3})?;?\s?\$?(\d{1,3},\d{3})?\s?(to|-)?\s?\$?(\d{1,3},\d{3})?;?",
            file_content)
        lower_band, upper_band = None, None
        if salary_pattern:
            lower_band = float(salary_pattern.group(1).replace(',',''))
            if salary_pattern.group(9):
                upper_band = float(salary_pattern.group(9).replace(',',''))
            elif salary_pattern.group(6):
                upper_band = float(salary_pattern.group(6).replace(',',''))
            else:
                upper_band = float(salary_pattern.group(3).replace(',',''))
        return lower_band, upper_band
    except Exception as e:
        raise ValueError(f"Error extracting salary: {e}")

def extract_start_date(file_content):
    try:
        start_date_match = re.search(r"([Ss]tart [Dd]ate:)\s*(\d{1,2}-\d{1,2}-\d{4})", file_content)
        date = datetime.strptime(start_date_match.group(2), "%m-%d-%Y" ) if start_date_match else None
        return date
    except Exception as e:
        raise ValueError(f"Error extracting start date: {e}")

def extract_end_date(file_content):
    try:
        end_date_match = re.search(
            r"(January|Feburary|March|April|May|June|July|August|September|October|November|December)\s(\d{1,2},\s\d{4})",
            file_content)
        date = datetime.strptime(end_date_match.group(), "%B %d, %Y")
        return date
    except Exception as e:
        raise ValueError(f"Error extracting end date: {e}")

def extract_req(file_content):
    try:
        req_match = re.search(
            r"(JOB)?\s?REQUIREMENT(S)?:\n?(.*)",
            file_content,
            re.IGNORECASE)
        req = req_match.group(3).strip() if req_match else None
        return req
    except Exception as e:
        raise ValueError(f"Error extracting requirements: {e}")

def extract_notes(file_content):
    try:
        notes_match = re.search(
            r"(NOTE[S?]):(.*)",
            file_content,
            re.DOTALL | re.IGNORECASE)
        notes = notes_match.group(2).strip() if notes_match else None
        return notes
    except Exception as e:
        raise ValueError(f"Error extracting notes: {e}")

def extract_job_desc(file_content):
    try:
        job_desc_match = re.search(
            r"((ROLE|JOB) DESCRIPTION):\n(.*)",
            file_content,
            re.IGNORECASE)
        job_desc = job_desc_match.group(3).strip() if job_desc_match else None
        return job_desc
    except Exception as e:
        raise ValueError(f"Error extracting job description: {e}")

def extract_selection_process(file_content):
    try:
        selection_match = re.search(
            r"SELECTION(S)?\s?(PROCESS)?:(.*)",
            file_content,
            re.IGNORECASE)
        selection = selection_match.group(3).strip() if selection_match else None
        return selection
    except Exception as e:
        raise ValueError(f"Error extracting selection: {e}")

def extract_experience_length(file_content):
    try:
        experience_match = re.search(
            r"([0-9]|[0-3][0-9]|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\sYEAR[S?]\s?(OF)?(RELEVANT)?(EXPERIENCE)?",
            file_content,
            re.IGNORECASE)
        selection = experience_match.group(1).strip() if experience_match else None
        if not selection.isnumeric():
            selection = w2n.word_to_num(selection)
        return selection
    except Exception as e:
        raise ValueError(f"Error extracting experience length: {e}")

def extract_job_type(file_content):
    try:
        job_type_match = re.search(
            r"EMPLOYMENT TYPE:(.*)",
            file_content,
            re.IGNORECASE)
        job_type = job_type_match.group(1).strip() if job_type_match else None
        return job_type
    except Exception as e:
        raise ValueError(f"Error extracting job type: {e}")

def extract_education(file_content):
    try:
        education_match = re.search(
            r"(ASSOCIATE|BACHELOR|MASTER|DOCTORAL)'?S?\sDEGREE",
            file_content,
            re.IGNORECASE)
        education = education_match.group(1) if education_match else None
        return education
    except Exception as e:
        raise ValueError(f"Error extracting education: {e}")

def extract_application_location(file_content):
    try:
        education_match = re.search(
            r"(APPLICATION:)\s?(.*)",
            file_content,
            re.IGNORECASE)
        education = education_match.group(2).strip() if education_match else None
        return education
    except Exception as e:
        raise ValueError(f"Error extracting education: {e}")

def define_udfs():
    return {
        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_job_code_udf': udf(extract_job_code, StringType()),
        'extract_salary_udf_udf': udf(extract_salary, StructType([
            StructField('salary_start', DoubleType(), True),
            StructField('salary_end', DoubleType(), True)
        ])),
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_end_date_udf': udf(extract_end_date, DateType()),
        'extract_req_udf': udf(extract_req, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_job_desc_udf': udf(extract_job_desc, StringType()),
        'extract_selection_process_udf': udf(extract_selection_process, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_job_type_udf': udf(extract_job_type, StringType()),
        'extract_education_udf': udf(extract_education, StringType()),
        'extract_application_length_udf': udf(extract_application_location, StringType())
    }


if __name__ == "__main__":
    json_input_dir = 'file:///opt/bitnami/spark/jobs/input/json'
    text_input_dir = 'file:///opt/bitnami/spark/jobs/input/text'

    schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("position", StringType(), True),
        StructField("job_code", StringType(), True),
        StructField("salary_start", DoubleType(), True),
        StructField("salary_end", DoubleType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("job_requirements", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("job_description", StringType(), True),
        StructField("selection_process", StringType(), True),
        StructField("experience_length", StringType(), True),
        StructField("job_type", StringType(), True),
        StructField("education", StringType(), True),
        StructField("application_location", StringType(), True)
    ])

    udfs = define_udfs()

    spark = (SparkSession.builder
              .appName("AWS_Spark_Unstructured")
              .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,"
                                             "com.amazonaws:aws-java-sdk:1.11.469,"
                                             "com.fasterxml.jackson.core:jackson-databind:2.15.3,"
                                             "commons-logging:commons-logging:1.2")
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
              .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
              .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
              .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
              .getOrCreate())


    json_df = (spark.readStream.json(json_input_dir, schema=schema, multiLine=True))                                                    # JSON Format needs schema unlike Spark format stream
    text_df = (spark.readStream
                    .format('text')
                    .option('wholetext', 'true')                                                                              # Read each text file as a single row
                    .load(text_input_dir))

    text_df = text_df.withColumn('file_name', udfs['extract_file_name_udf']('value')) \
                     .withColumn('position', udfs['extract_position_udf']('value')) \
                     .withColumn('job_code', udfs['extract_job_code_udf']('value')) \
                     .withColumn('salary_start', udfs['extract_salary_udf_udf']('value').getField("salary_start")) \
                     .withColumn('salary_end', udfs['extract_salary_udf_udf']('value').getField("salary_end")) \
                     .withColumn('start_date', udfs['extract_start_date_udf']('value')) \
                     .withColumn('end_date', udfs['extract_end_date_udf']('value')) \
                     .withColumn('job_requirements', udfs['extract_req_udf']('value')) \
                     .withColumn('notes', udfs['extract_notes_udf']('value')) \
                     .withColumn('job_description', udfs['extract_job_desc_udf']('value')) \
                     .withColumn('selection_process', udfs['extract_selection_process_udf']('value')) \
                     .withColumn('experience_length', udfs['extract_experience_length_udf']('value')) \
                     .withColumn('job_type', udfs['extract_job_type_udf']('value')) \
                     .withColumn('education', udfs['extract_education_udf']('value')) \
                     .withColumn('application_location', udfs['extract_application_length_udf']('value'))


    select_fields = 'file_name', 'position', 'job_code', 'start_date', 'end_date', 'salary_start', 'salary_end', 'job_requirements', 'notes', \
                   'job_description', 'selection_process', 'experience_length', 'job_type', 'education', 'application_location'

    json_df = json_df.select(*select_fields)
    text_df = text_df.select(*select_fields)
    final_df = json_df.union(text_df)


    # query = (final_df.writeStream
    #             .format('console')
    #             .outputMode('append')
    #             .option('truncate', False)
    #             .start() )

    def streamWriter(input:DataFrame, checkpointFolder, output):
        return (input.writeStream
                        .format('parquet')
                        .option('checkpointLocation', checkpointFolder)
                        .option('path', output)
                        .outputMode('append')
                        .trigger(processingTime='5 seconds')
                        .start())

    query = streamWriter(final_df, 's3a://job-post-stream/checkpoints', 's3a://job-post-stream/data/unstructured')

    query.awaitTermination()

    spark.stop()