from pyspark.sql.functions import *

from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark import StorageLevel
import argparse
import common_utils as commonUtils


def analysis_1(primary_person_df):
    """
    :param primary_person_df: primary_person_use
    :return: This function returns the total number of crashes (accidents) in which number of persons killed are male
    """
    crash_involves_man_df = primary_person_df.filter((primary_person_df['PRSN_INJRY_SEV_ID'] == 'KILLED')
                                                     & (primary_person_df['PRSN_GNDR_ID'] == 'MALE'))
    result_df = crash_involves_man_df.select(crash_involves_man_df['CRASH_ID']).dropDuplicates()
    return '''Number of crashes in which male person killed: {}'''.format(result_df.count())


def analysis_2(units_use_df):
    """
    :param units_use_df: units_use_case dataframe
    :return: returns the count of two wheelers booked for crashes
    """
    two_wheelers_booked_df = units_use_df.filter(units_use_df['VEH_BODY_STYL_ID'] == 'MOTORCYCLE')
    # total_cnt = two_wheelers_booked_df.select(two_wheelers_booked_df['VIN']).dropDuplicates().count()
    total_cnt = two_wheelers_booked_df.count()
    return '''Two wheelers booked for crashes: {}'''.format(total_cnt)


def analysis_3(units_use_df, primary_person_df):
    """
    :param units_use_df: units_use_case dataframe
    :param primary_person_df: primary_person_use
    :return: returns the state that has highest number of accidents in which females are involved
    """
    crash_state_wise_df = units_use_df.select(units_use_df['CRASH_ID'], units_use_df['VEH_LIC_STATE_ID']) \
        .dropDuplicates()

    female_crash_df = primary_person_df.filter(primary_person_df['PRSN_GNDR_ID'] == 'FEMALE') \
        .select(primary_person_df['CRASH_ID']).dropDuplicates()

    crash_state_wise_female_df = crash_state_wise_df.join(female_crash_df, how='inner', on='CRASH_ID')
    crash_state_wise_female_df.repartition(18)
    crash_state_wise_female_df = crash_state_wise_female_df.groupBy('VEH_LIC_STATE_ID').agg({'CRASH_ID': 'count'}) \
        .withColumnRenamed('count(CRASH_ID)', 'crash_count_by_state').sort('crash_count_by_state', ascending=False)
    result = crash_state_wise_female_df.head()
    return '''{} state has highest number of accidents({}) involving females.'''.format(result[0], result[1])


def analysis_4(units_use_df):
    """
    returns Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    :param units_use_df:
    :return:
    """
    injury_death_df = units_use_df.filter(col('VEH_MAKE_ID') != 'NA').select('VEH_MAKE_ID', 'TOT_INJRY_CNT',
                                                                             'DEATH_CNT') \
        .withColumn('death_injury_count', (col('TOT_INJRY_CNT') + col('DEATH_CNT')).cast(IntegerType()))
    injury_death_df = injury_death_df.groupBy('VEH_MAKE_ID').agg({'death_injury_count': 'sum'})

    injuryWindowSpec = Window.orderBy(col('sum(death_injury_count)').desc())

    injury_death_df = injury_death_df.withColumn('injury_rank', rank().over(injuryWindowSpec)) \
        .filter((col('injury_rank') >= 5) & (col('injury_rank') <= 15))
    return injury_death_df


def analysis_5(primary_person_df, units_use_df):
    """
    For all the body styles involved in crashes, return the top ethnic user group of each unique body style
    :param primary_person_df:
    :param units_use_df:
    :return:
    """
    vehicle_body_df = units_use_df.select("CRASH_ID", "UNIT_NBR", "VEH_BODY_STYL_ID")
    ethnic_df = primary_person_df.select("CRASH_ID", "UNIT_NBR", "PRSN_ETHNICITY_ID")
    body_ethnic_df = vehicle_body_df.join(ethnic_df, [ethnic_df["CRASH_ID"] == vehicle_body_df["CRASH_ID"],
                                                      ethnic_df["UNIT_NBR"] == vehicle_body_df["UNIT_NBR"]],
                                          'left_outer')
    body_ethnic_df = body_ethnic_df.select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
    body_ethnic_df = body_ethnic_df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').agg({'PRSN_ETHNICITY_ID': 'count'})

    body_ethnic_windowSpec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count(PRSN_ETHNICITY_ID)').desc())

    body_ethnic_df = body_ethnic_df.withColumn('body_ethnic_rank', rank().over(body_ethnic_windowSpec)).filter(
        col('body_ethnic_rank') == 1)
    return body_ethnic_df


def analysis_6(primary_person_df, units_use_df):
    """
    Among the crashed cars, returns the Top 5 Zip Codes with highest number crashes with alcohols as the contributing
    factor to a crash (Use Driver Zip Code)
    :param primary_person_df:
    :param units_use_df:
    :return:
    """
    alcolhol_crash_df = primary_person_df.filter(
        (col('PRSN_ALC_RSLT_ID') == 'Positive') & (col('DRVR_ZIP').isNotNull())).select('CRASH_ID', 'UNIT_NBR',
                                                                                        'DRVR_ZIP')
    cars_df = units_use_df.filter(col('VEH_BODY_STYL_ID').contains('CAR')).select("CRASH_ID", "UNIT_NBR",
                                                                                  "VEH_BODY_STYL_ID")
    alcolhol_crash_df = alcolhol_crash_df.join(cars_df, [alcolhol_crash_df["CRASH_ID"] == cars_df["CRASH_ID"],
                                                         alcolhol_crash_df["UNIT_NBR"] == cars_df["UNIT_NBR"]], 'inner')
    alcolhol_crash_df = alcolhol_crash_df.groupBy('DRVR_ZIP').count()
    alcohol_crash_window = Window.orderBy(col('count').desc())
    alcolhol_crash_df = alcolhol_crash_df.withColumn('zip_rank', rank().over(alcohol_crash_window)).filter(
        col('zip_rank') <= 5)
    return alcolhol_crash_df


def analysis_7(damages_use_df, units_use_df):
    """
    Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
    and car avails Insurance
    :param damages_use_df:
    :param units_use_df:
    :return:
    """
    damage_cars_df = units_use_df.filter(
        (col('VEH_BODY_STYL_ID').contains('CAR')) & ((col('FIN_RESP_TYPE_ID')) != 'NA'))
    damage_cars_df = damage_cars_df.filter((regexp_extract(col('VEH_DMAG_SCL_1_ID'), "[5-9]+", 0) != '') | (
            regexp_extract(col('VEH_DMAG_SCL_2_ID'), "[5-9]+", 0) != ''))
    damage_cars_df = damage_cars_df.select('CRASH_ID').dropDuplicates()
    crash_id_df = damage_cars_df.join(damages_use_df, how='left_anti', on='CRASH_ID')
    return '''Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car
    avails Insurance:{}'''.format(crash_id_df.count())


def top_offence_states_lkp(units_use_df):
    """
    :param units_use_df:
    :return: top 25 states with highest number of offences
    """
    top_offence_states_df = units_use_df.filter(col('VEH_LIC_STATE_ID') != 'NA').select('CRASH_ID', 'UNIT_NBR',
                                                                                        "VEH_LIC_STATE_ID").dropDuplicates()
    top_offence_states_df = top_offence_states_df.groupBy('VEH_LIC_STATE_ID').count()

    top_offence_window = Window.orderBy(col('count').desc())

    top_offence_states_df = top_offence_states_df.withColumn('offence_rank', rank().over(top_offence_window))
    return top_offence_states_df.filter(col('offence_rank') <= 25).select('VEH_LIC_STATE_ID')


def top_vehicle_colors_lkp(units_use_df):
    """
    :param units_use_df:
    :return: returns Top 10 used vehicle colors
    """
    top_vehicle_colors_df = units_use_df.filter(col('VEH_COLOR_ID') != 'NA').select('CRASH_ID', 'UNIT_NBR',
                                                                                    "VEH_COLOR_ID").dropDuplicates()
    top_vehicle_colors_df = top_vehicle_colors_df.groupBy('VEH_COLOR_ID').count()
    top_vehicle_colors_window = Window.orderBy(col('count').desc())
    top_vehicle_colors_df = top_vehicle_colors_df.withColumn('color_rank', rank().over(top_vehicle_colors_window))
    return top_vehicle_colors_df.filter(col('color_rank') <= 10).select('VEH_COLOR_ID')


def analysis_8(units_use_df, restrict_use_df, charges_use_df):
    """
    : Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
    has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest
    number of offences
    :param units_use_df:
    :param restrict_use_df:
    :param charges_use_df:
    :return:
    """
    top_offence_states_df = top_offence_states_lkp(units_use_df)
    top_vehicle_colors_df = top_vehicle_colors_lkp(units_use_df)

    # licenced drivers
    licenced_drivers_df = restrict_use_df.filter(col('DRVR_LIC_RESTRIC_ID') != 'UNLICENSED')
    licenced_drivers_df = licenced_drivers_df.select('CRASH_ID', 'UNIT_NBR').dropDuplicates()

    # drivers charged with speed related offences
    speed_offence_df = charges_use_df.filter(col('CHARGE').contains('SPEED'))
    speed_offence_df = speed_offence_df.select('CRASH_ID', 'UNIT_NBR').dropDuplicates()

    # licenced drivers with speed related offence
    licenced_driver_speed_offence_df = licenced_drivers_df.join(speed_offence_df, [
        licenced_drivers_df["CRASH_ID"] == speed_offence_df["CRASH_ID"],
        licenced_drivers_df["UNIT_NBR"] == speed_offence_df["UNIT_NBR"]], 'inner')
    licenced_driver_speed_offence_df = licenced_driver_speed_offence_df.select(licenced_drivers_df['CRASH_ID'],
                                                                               licenced_drivers_df['UNIT_NBR'])

    # uses top 10 vehicle colors, car licence with top 25 states with offence
    vehicles_df = units_use_df.filter(col('VEH_BODY_STYL_ID').contains('CAR'))
    vehicles_df = vehicles_df.join(broadcast(top_vehicle_colors_df), how='inner', on='VEH_COLOR_ID')
    vehicles_df = vehicles_df.join(broadcast(top_offence_states_df), how='inner', on='VEH_LIC_STATE_ID')
    vehicles_df = vehicles_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID').dropDuplicates()

    # join to get final data
    final_df = licenced_driver_speed_offence_df.join(vehicles_df, [
        licenced_driver_speed_offence_df["CRASH_ID"] == vehicles_df["CRASH_ID"],
        licenced_driver_speed_offence_df["UNIT_NBR"] == vehicles_df["UNIT_NBR"]], 'inner')
    final_df = final_df.groupBy('VEH_MAKE_ID').count()
    final_Window = Window.orderBy(col('count').desc())
    result = final_df.withColumn('final_rank', rank().over(final_Window)).filter(col('final_rank') <= 5)
    return result.select('VEH_MAKE_ID')


def process(spark, utils, args):
    """
    This function calls different analytical functions to meet the requirements
    :param spark: spark session object
    :param utils: utils object to use common utils
    :param args: command line arguments
    :return: None
    """
    ip_dir = args.input_dir
    op_dir = args.output_dir

    # set i/p paths
    primary_person_ip_path = ip_dir + "/Primary_Person_use.csv"
    units_use_ip_path = ip_dir + "/Units_use.csv"
    damages_use_ip_path = ip_dir + "/Damages_use.csv"
    restrict_use_ip_path = ip_dir + "/Restrict_use.csv"
    charges_use_ip_path = ip_dir + "/Charges_use.csv"

    primary_person_df = utils.read_csv(spark, primary_person_ip_path)
    primary_person_df.repartition(10)
    primary_person_df.persist(StorageLevel.MEMORY_AND_DISK)

    analysis_1_result = analysis_1(primary_person_df)
    utils.write_text_data(analysis_1_result, op_dir + "/analysis_1_op.txt")

    units_use_df = utils.read_csv(spark, units_use_ip_path).dropDuplicates()
    units_use_df.repartition(10)
    units_use_df.persist(StorageLevel.MEMORY_AND_DISK)

    analysis_2_result = analysis_2(units_use_df)
    utils.write_text_data(analysis_2_result, op_dir + "/analysis_2_op.txt")

    analysis_3_result = analysis_3(units_use_df, primary_person_df)
    utils.write_text_data(analysis_3_result, op_dir + "/analysis_3_op.txt")

    analysis_4_df = analysis_4(units_use_df)
    utils.write_csv(analysis_4_df, op_dir + "/analysis_4_op")

    analysis_5_df = analysis_5(primary_person_df, units_use_df)
    utils.write_csv(analysis_5_df, op_dir + "/analysis_5_op")

    analysis_6_df = analysis_6(primary_person_df, units_use_df)
    utils.write_csv(analysis_6_df, op_dir + "/analysis_6_op")

    damages_use_df = utils.read_csv(spark, damages_use_ip_path)

    analysis_7_df = analysis_7(damages_use_df, units_use_df)
    utils.write_text_data(analysis_7_df, op_dir + "/analysis_7_op.txt")

    restrict_use_df = utils.read_csv(spark, restrict_use_ip_path)
    charges_use_df = utils.read_csv(spark, charges_use_ip_path)

    analysis_8_df = analysis_8(units_use_df, restrict_use_df, charges_use_df)
    utils.write_csv(analysis_8_df, op_dir + "/analysis_8_op")


if __name__ == '__main__':
    """
    Main method, execution starts from here.
    Parses command line arguments
    creates spark session object and common utils object
    calls process() method to execute the requirements
    """
    try:
        parser = argparse.ArgumentParser(description='Car Crash Analysis')
        parser.add_argument('-id', "--input_dir", help="directory for input files", required=False, default="./Data")
        parser.add_argument('-od', "--output_dir", help="directory for output files", required=False,
                            default="./OutputData")
        args = parser.parse_args()
        utils = commonUtils.commonUtils()
        spark = utils.create_session()
        process(spark, utils, args)
    except Exception as e:
        print("Error occurred!")
        print(e)
        exit(0)
