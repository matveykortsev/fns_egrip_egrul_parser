from pyspark.sql import functions as f
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, ArrayType
import os
from datetime import datetime


def getSparkSessionInstance():
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .master("local[*]") \
            .config("spark.sql.caseSensitive", "true") \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def check_column_exists(col_name: str, df):
    try:
        df.select(col_name)
        return f.col(col_name)
    except Exception as e:
        print(f'Column {col_name} does not exist. Fill null')
        return f.lit(None).cast('string')


def get_cols_list(schema: StructType) -> list:
    res_list = []
    for field in schema:
        if isinstance(field.dataType, StructType):
            res_list += [".".join([field.name, str(col)]) for col in get_cols_list(field.dataType)]
        else:
            res_list.append(field.name)
    return res_list


def get_arrays(schema: StructType, depth: int = None) -> list:
    res_list = []
    for field in schema:
        if isinstance(field.dataType, StructType):
            res_list += [".".join([field.name, str(col)]) for col in get_arrays(field.dataType)]
        elif isinstance(field.dataType, ArrayType):
            res_list.append(field.name)
            if not depth:
                continue
            if isinstance(field.dataType.elementType, StructType):
                res_list += [".".join([field.name, str(col)]) for col in
                             get_arrays(field.dataType.elementType, depth - 1)]
    return res_list


def spark_read(df):
    arrays = get_arrays(df.schema)
    print("arrays", arrays)
    explode_expr = list(map(lambda x: (x, f.explode_outer(x).alias(str("_" + x).replace(".", "_"))), arrays))

    for expr in explode_expr:
        print(expr[0])
        df = df.select("*", expr[1])
        df = df.drop(f.col(expr[0]))
    arrays = set(get_arrays(df.schema)).difference(set(arrays))
    explode_expr = list(map(lambda x: (x, f.explode_outer(x).alias(str(x).replace(".", "_"))), arrays))
    print("set_arrays", arrays)
    for expr in explode_expr:
        print(expr[0])
        df = df.select(f.col("*"), expr[1])
        df = df.drop(f.col(expr[0]))
    cols = get_cols_list(df.schema)
    struct_expr = list(map(lambda x: f.col(x).alias(str(x).replace(".", "_")), cols))
    df = df.select(*struct_expr)

    df.printSchema()
    return df


def rename_parquet(source_dir, dest_dir, p_name):
    for file in os.listdir(source_dir):
        if file.endswith('.parquet'):
            new_file = os.path.join(dest_dir, '{}.parquet'.format(p_name))
            os.rename(os.path.join(source_dir, file), new_file)


def create_main_parquet(spark, xml_path, parquet_path, flag):
    if flag == 'egrul':
        df = spark.read.format("xml") \
            .option("rootTag", "EGRUL") \
            .option("rowTag", "СвЮЛ") \
            .option("charset", "cp1251") \
            .load(path=xml_path)

    elif flag == 'egrip':
        df = spark.read.format("xml") \
            .option("rootTag", "EGRIP") \
            .option("rowTag", "СвИП") \
            .option("charset", "cp1251") \
            .load(path=xml_path)

    df = df.withColumn('table_guid', f.expr("uuid()")) \
        .withColumn('file_name', f.lit(os.path.basename(xml_path))) \
        .withColumn('file_date', f.lit(os.path.basename(xml_path.split('_')[-2].replace('-', '.')))) \
        .withColumn('load_date', f.lit(datetime.now()).cast('string')) \
        .withColumn('folder_date', f.lit(os.path.basename(xml_path.split('_')[-2].replace('-', '.'))))

    df.distinct() \
        .repartition(1) \
        .write \
        .option("parquet.block.size", 256 * 1024 * 1024) \
        .mode("overwrite") \
        .parquet(parquet_path)


def read_parquet(file):
    spark = getSparkSessionInstance()
    return spark.read.format('parquet').load(file)


def save_parquet(df, path):
    df.distinct() \
        .repartition(1) \
        .write \
        .option("parquet.block.size", 256 * 1024 * 1024) \
        .mode("overwrite") \
        .parquet(path)


def merge_parquets(parquets_list_to_merge):
    spark = getSparkSessionInstance()
    return spark.read.option("mergeSchema", "true").parquet(*parquets_list_to_merge)


def create_ip_info(df):
    ip_info = df.select(f.col('_ДатаВып').alias('ip_date').cast('string'),
                        f.col('_ОГРНИП').alias('ogrnip').cast('string'),
                        f.col('_ДатаОГРНИП').alias('ogrnip_date').cast('string'),
                        f.col('_ИННФЛ').alias('innfl').cast('string'),
                        f.col('_КодВидИП').alias('vid_ip_code').cast('string'),
                        f.col('_НаимВидИП').alias('vid_ip_name').cast('string'),
                        f.col('table_guid'),
                        f.col('file_name'),
                        f.col('file_date'),
                        f.col('load_date'),
                        f.col('folder_date'))
    return ip_info


def create_fl_info(df):
    fl_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвФЛ.*')))

    fl_info = fl_info.select(f.col('table_guid').alias('rodtable_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                f.col('_Пол').alias('gender').cast('string'),
                f.col('ФИОРус__Фамилия').alias('surname_rus').cast('string'),
                f.col('ФИОРус__Имя').alias('name_rus').cast('string'),
                f.col('ФИОРус__Отчество').alias('patronymic_rus').cast('string'),
                check_column_exists('ФИОЛат__Фамилия', fl_info).alias('surname_lat').cast('string'),
                check_column_exists('ФИОЛат__Имя', fl_info).alias('name_lat').cast('string'),
                check_column_exists('ФИОЛат__Отчество', fl_info).alias('patronymic_lat').cast('string'),
                f.col('ГРНИПДата__ГРНИП').alias('egrip_num').cast('string'),
                f.col('ГРНИПДата__ДатаЗаписи').alias('egrip_date').cast('string'),
                check_column_exists('ГРНИПДатаИспр__ГРНИП', fl_info).alias('egrip_teh_num').cast('string'),
                check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', fl_info).alias('egrip_teh_date').cast('string')
                ).withColumn('table_guid', f.expr("uuid()"))
    return fl_info


def create_citizenship_info(df):
    citizenship_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвГражд.*')))

    citizenship_info = citizenship_info.select(f.col('table_guid').alias('rodtable_guid'),
                                               f.col('load_date'),
                                               f.col('folder_date'),
                                               check_column_exists('_ВидГражд', citizenship_info).alias(
                                                   'сitizenship_type').cast('string'),
                                               check_column_exists('_ОКСМ', citizenship_info).alias('oksm').cast(
                                                   'string'),
                                               check_column_exists('_НаимСтран', citizenship_info).alias(
                                                   'country').cast('string'),
                                               check_column_exists('ГРНИПДата__ГРНИП', citizenship_info).alias(
                                                   'egrip_num').cast('string'),
                                               check_column_exists('ГРНИПДата__ДатаЗаписи', citizenship_info).alias(
                                                   'egrip_date').cast('string'),
                                               check_column_exists('ГРНИПДатаИспр__ГРНИП', citizenship_info).alias(
                                                   'egrip_teh_num').cast('string'),
                                               check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', citizenship_info).alias(
                                                   'egrip_teh_date').cast('string')
                                               ).withColumn('table_guid', f.expr("uuid()"))
    return citizenship_info


def create_regip_info(df):
    regip_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвРегИП.*')))

    regip_info = regip_info.select(f.col('table_guid').alias('rodtable_guid'),
                                   f.col('load_date'),
                                   f.col('folder_date'),
                                   check_column_exists('_ОГРНИП', regip_info).alias('ogrnip').cast('string'),
                                   check_column_exists('_ДатаОГРНИП', regip_info).alias('ogrnip_date').cast('string'),
                                   check_column_exists('_РегНом', regip_info).alias('reg_number').cast('string'),
                                   check_column_exists('_ДатаРег', regip_info).alias('reg_date').cast('string'),
                                   check_column_exists('_НаимРО', regip_info).alias('ro_name').cast('string'),
                                   check_column_exists('СвКФХ__ОГРН', regip_info).alias('ogrn').cast('string'),
                                   check_column_exists('СвКФХ__ИНН', regip_info).alias('inn').cast('string'),
                                   check_column_exists('СвКФХ__НаимЮЛПолн', regip_info).alias('ul_name').cast('string'),
                                   check_column_exists('СвКФХ_ГРНИПДата__ГРНИП', regip_info).alias('egrip_num').cast(
                                       'string'),
                                   check_column_exists('СвКФХ_ГРНИПДата__ДатаЗаписи', regip_info).alias(
                                       'egrip_date').cast('string'),
                                   check_column_exists('СвКФХ_ГРНИПДатаИспр__ГРНИП', regip_info).alias(
                                       'egrip_teh_num').cast('string'),
                                   check_column_exists('СвКФХ_ГРНИПДатаИспр__ДатаЗаписи', regip_info).alias(
                                       'egrip_teh_date').cast('string')
                                   ).withColumn('table_guid', f.expr("uuid()"))
    return regip_info


def create_regorg_info(df):
    regorg_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвРегОрг.*')))

    regorg_info = regorg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                     f.col('load_date'),
                                     f.col('folder_date'),
                                     check_column_exists('_КодНО', regorg_info).alias('no_code').cast('string'),
                                     check_column_exists('_НаимНО', regorg_info).alias('no_name').cast('string'),
                                     check_column_exists('_АдрРО', regorg_info).alias('no_address').cast('string'),
                                     check_column_exists('ГРНИПДата__ГРНИП', regorg_info).alias('egrip_num').cast(
                                         'string'),
                                     check_column_exists('ГРНИПДата__ДатаЗаписи', regorg_info).alias('egrip_date').cast(
                                         'string')
                                     ).withColumn('table_guid', f.expr("uuid()"))

    return regorg_info


def create_status_info(df):
    status_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвСтатус.*', df)))

    status_info = status_info.select(f.col('table_guid').alias('rodtable_guid'),
                                     f.col('load_date'),
                                     f.col('folder_date'),
                                     check_column_exists('СвСтатус__КодСтатус', status_info).alias('status_code').cast(
                                         'string'),
                                     check_column_exists('СвСтатус__НаимСтатус', status_info).alias('status_name').cast(
                                         'string'),
                                     check_column_exists('ГРНИПДата__ГРНИП', status_info).alias('egrip_num').cast(
                                         'string'),
                                     check_column_exists('ГРНИПДата__ДатаЗаписи', status_info).alias('egrip_date').cast(
                                         'string'),
                                     check_column_exists('СвРешИсклИП__ДатаРеш', status_info).alias(
                                         'decision_ex_date').cast('string'),
                                     check_column_exists('СвРешИсклИП__НомерРеш', status_info).alias(
                                         'decision_ex_num').cast('string'),
                                     check_column_exists('СвРешИсклИП__ДатаПубликации', status_info).alias(
                                         'decision_ex_publ_date').cast('string'),
                                     check_column_exists('СвРешИсклИП__НомерЖурнала', status_info).alias(
                                         'decision_ex_jour').cast('string')
                                     ).withColumn('table_guid', f.expr("uuid()"))

    return status_info


def create_stop_info(df):
    stop_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвПрекращ.*', df)))

    stop_info = stop_info.select(f.col('table_guid').alias('rodtable_guid'),
                                 f.col('load_date'),
                                 f.col('folder_date'),
                                 check_column_exists('СвСтатус__КодСтатус', stop_info).alias('status_code').cast(
                                     'string'),
                                 check_column_exists('СвСтатус__НаимСтатус', stop_info).alias('status_name').cast(
                                     'string'),
                                 check_column_exists('СвСтатус__ДатаПрекращ', stop_info).alias('stop_date').cast(
                                     'string'),
                                 check_column_exists('ГРНИПДата__ГРНИП', stop_info).alias('egrip_num').cast('string'),
                                 check_column_exists('ГРНИПДата__ДатаЗаписи', stop_info).alias('egrip_date').cast(
                                     'string'),
                                 check_column_exists('СвНовЮЛ__ОГРН', stop_info).alias('ogrn').cast('string'),
                                 check_column_exists('СвНовЮЛ__ИНН', stop_info).alias('inn').cast('string'),
                                 check_column_exists('СвНовЮЛ__НаимЮЛПолн', stop_info).alias('ul_full_name').cast(
                                     'string'),
                                 check_column_exists('СвНовЮЛ_ГРНИПДата__ГРНИП', stop_info).alias('egrip_dop_num').cast(
                                     'string'),
                                 check_column_exists('СвНовЮЛ_ГРНИПДата__ДатаЗаписи', stop_info).alias(
                                     'egrip_dop_date').cast('string'),
                                 check_column_exists('СвНовЮЛ_ГРНИПДатаИспр__ГРНИП', stop_info).alias(
                                     'egrip_teh_num').cast('string'),
                                 check_column_exists('СвНовЮЛ_ГРНИПДатаИспр__ДатаЗаписи', stop_info).alias(
                                     'egrip_teh_date').cast('string')
                                 ).withColumn('table_guid', f.expr("uuid()")).withColumn('ul_full_name', f.regexp_replace(f.col('ul_full_name'), '[\n\r\t]', ''))

    return stop_info


def create_uchetno_info(df):
    uchetno_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвУчетНО.*')))

    uchetno_info = uchetno_info.select(f.col('table_guid').alias('rodtable_guid'),
                                       f.col('load_date'),
                                       f.col('folder_date'),
                                       check_column_exists('_ИННФЛ', uchetno_info).alias('innfl').cast('string'),
                                       check_column_exists('_ДатаПостУч', uchetno_info).alias('uchet_date').cast(
                                           'string'),
                                       check_column_exists('СвНО__КодНО', uchetno_info).alias('no_code').cast('string'),
                                       check_column_exists('СвНО__НаимНО', uchetno_info).alias('no_name').cast(
                                           'string'),
                                       check_column_exists('ГРНИПДата__ГРНИП', uchetno_info).alias('egrip_num').cast(
                                           'string'),
                                       check_column_exists('ГРНИПДата__ДатаЗаписи', uchetno_info).alias(
                                           'egrip_date').cast('string'),
                                       check_column_exists('ГРНИПДатаИспр__ГРНИП', uchetno_info).alias(
                                           'egrip_teh_num').cast('string'),
                                       check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', uchetno_info).alias(
                                           'egrip_teh_date').cast('string')
                                       ).withColumn('table_guid', f.expr("uuid()"))

    return uchetno_info


def create_regpf_info(df):
    regpf_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвРегПФ.*')))

    regpf_info = regpf_info.select(f.col('table_guid').alias('rodtable_guid'),
                                   f.col('load_date'),
                                   f.col('folder_date'),
                                   check_column_exists('_РегНомПФ', regpf_info).alias('regpf_num').cast('string'),
                                   check_column_exists('_ДатаРег', regpf_info).alias('reg_date').cast('string'),
                                   check_column_exists('СвОргПФ__КодПФ', regpf_info).alias('pf_code').cast('string'),
                                   check_column_exists('СвОргПФ__НаимПФ', regpf_info).alias('pf_name').cast('string'),
                                   check_column_exists('ГРНИПДата__ГРНИП', regpf_info).alias('regpf_grnip').cast(
                                       'string'),
                                   check_column_exists('ГРНИПДата__ДатаЗаписи', regpf_info).alias('regpf_date').cast(
                                       'string'),
                                   check_column_exists('ГРНИПДатаИспр__ГРНИП', regpf_info).alias(
                                       'regpf_red_grnip').cast('string'),
                                   check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', regpf_info).alias(
                                       'regpf_red_date').cast('string')
                                   ).withColumn('table_guid', f.expr("uuid()"))

    return regpf_info


def create_regfss_info(df):
    regfss_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвРегФСС.*', df)))

    regfss_info = regfss_info.select(f.col('table_guid').alias('rodtable_guid'),
                                     f.col('load_date'),
                                     f.col('folder_date'),
                                     check_column_exists('_РегНомФСС', regfss_info).alias('fssreg_num').cast('string'),
                                     check_column_exists('_ДатаРег', regfss_info).alias('fssreg_date').cast('string'),
                                     check_column_exists('СвОргФСС__КодФСС', regfss_info).alias('fss_code').cast(
                                         'string'),
                                     check_column_exists('СвОргФСС__НаимФСС', regfss_info).alias('fss_name').cast(
                                         'string'),
                                     check_column_exists('ГРНИПДата__ГРНИП', regfss_info).alias('egrip_num').cast(
                                         'string'),
                                     check_column_exists('ГРНИПДата__ДатаЗаписи', regfss_info).alias('egrip_date').cast(
                                         'string'),
                                     check_column_exists('ГРНИПДатаИспр__ГРНИП', regfss_info).alias(
                                         'egrip_teh_num').cast('string'),
                                     check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', regfss_info).alias(
                                         'egrip_teh_date').cast('string')
                                     ).withColumn('table_guid', f.expr("uuid()"))

    return regfss_info


def create_okved_info(df):
    okved_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвОКВЭД.*')))

    okved_info = okved_info.select(f.col('table_guid').alias('rodtable_guid'),
                                   f.col('load_date'),
                                   f.col('folder_date'),
                                   check_column_exists('СвОКВЭДОсн__КодОКВЭД', okved_info).alias('okved_code').cast(
                                       'string'),
                                   check_column_exists('СвОКВЭДОсн__НаимОКВЭД', okved_info).alias('okved_name').cast(
                                       'string'),
                                   check_column_exists('СвОКВЭДОсн__ПрВерсОКВЭД', okved_info).alias('okved_ver').cast(
                                       'string'),
                                   check_column_exists('СвОКВЭДОсн_ГРНИПДата__ГРНИП', okved_info).alias(
                                       'egrip_num').cast('string'),
                                   check_column_exists('СвОКВЭДОсн_ГРНИПДата__ДатаЗаписи', okved_info).alias(
                                       'egrip_date').cast('string'),
                                   check_column_exists('СвОКВЭДОсн_ГРНИПДатаИспр__ГРНИП', okved_info).alias(
                                       'egrip_teh_num').cast('string'),
                                   check_column_exists('СвОКВЭДОсн_ГРНИПДатаИспр__ДатаЗаписи', okved_info).alias(
                                       'egrip_teh_date').cast('string'),
                                   check_column_exists('_СвОКВЭДДоп__КодОКВЭД', okved_info).alias('okveddop_code').cast(
                                       'string'),
                                   check_column_exists('_СвОКВЭДДоп__НаимОКВЭД', okved_info).alias(
                                       'okveddop_name').cast('string'),
                                   check_column_exists('_СвОКВЭДДоп__ПрВерсОКВЭД', okved_info).alias(
                                       'okveddop_ver').cast('string'),
                                   check_column_exists('_СвОКВЭДДоп_ГРНИПДата__ГРНИП', okved_info).alias(
                                       'egrip_dop_num').cast('string'),
                                   check_column_exists('_СвОКВЭДДоп_ГРНИПДата__ДатаЗаписи', okved_info).alias(
                                       'egrip_dop_date').cast('string'),
                                   check_column_exists('_СвОКВЭДДоп_ГРНИПДатаИспр__ГРНИП', okved_info).alias(
                                       'egrip_teh_dop_num').cast('string'),
                                   check_column_exists('_СвОКВЭДДоп_ГРНИПДатаИспр__ДатаЗаписи', okved_info).alias(
                                       'egrip_teh_dop_date').cast('string')
                                   ).withColumn('table_guid', f.expr("uuid()"))

    return okved_info


def create_lic_info(df):
    lic_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвЛицензия', df)))
    if 'СвЛицензия' in get_arrays(df.select(check_column_exists('СвЛицензия', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                check_column_exists('_СвЛицензия__СерЛиц', lic_info).alias('lic_ser').cast('string'),
                check_column_exists('_СвЛицензия__НомЛиц', lic_info).alias('lic_num').cast('string'),
                check_column_exists('_СвЛицензия__ВидЛиц', lic_info).alias('lic_type').cast(
                    'string'),
                check_column_exists('_СвЛицензия__ДатаЛиц', lic_info).alias('lic_date').cast(
                    'string'),
                check_column_exists('_СвЛицензия__ДатаНачЛиц', lic_info).alias(
                    'lic_start_date').cast(
                    'string'),
                check_column_exists('_СвЛицензия__ДатаОкончЛиц', lic_info).alias(
                    'lic_end_date').cast(
                    'string'),
                check_column_exists('_СвЛицензия_НаимЛицВидДеят', lic_info).alias('lic_dotype').cast(
                    'string'),
                check_column_exists('_СвЛицензия_МестоДейстЛиц', lic_info).alias('lic_place').cast(
                    'string'),
                check_column_exists('_СвЛицензия_ЛицОргВыдЛиц', lic_info).alias('lic_porg').cast(
                    'string'),
                check_column_exists('_СвЛицензия_ГРНИПДата__ГРНИП', lic_info).alias(
                    'egrip_num').cast(
                    'string'),
                check_column_exists('_СвЛицензия_ГРНИПДата__ДатаЗаписи', lic_info).alias(
                    'egrip_date').cast('string'),
                check_column_exists('_СвЛицензия_ГРНИПДатаИспр__ГРНИП', lic_info).alias(
                    'egrip_teh_num').cast('string'),
                check_column_exists('_СвЛицензия_ГРНИПДатаИспр__ДатаЗаписи', lic_info).alias(
                    'egrip_teh_date').cast('string'),
                check_column_exists('_СвЛицензия_СвПриостЛиц__ДатаПриостЛиц', lic_info).alias(
                    'lic_stop_date').cast('string'),
                check_column_exists('_СвЛицензия_СвПриостЛиц__ЛицОргПриостЛиц', lic_info).alias(
                    'lic_stop_org').cast('string'),
                check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНИПДата__ГРНИП', lic_info).alias(
                    'egrip_dop_num').cast('string'),
                check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНИПДата__ДатаЗаписи', lic_info).alias(
                    'egrip_dop_date').cast('string'),
                check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНИПДатаИспр__ГРНИП', lic_info).alias(
                    'egrip_teh_dop_num').cast('string'),
                check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНИПДатаИспр__ДатаЗаписи',
                                    lic_info).alias(
                    'egrip_teh_dop_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                check_column_exists('СвЛицензия__СерЛиц', lic_info).alias('lic_ser').cast('string'),
                check_column_exists('СвЛицензия__НомЛиц', lic_info).alias('lic_num').cast('string'),
                check_column_exists('СвЛицензия__ВидЛиц', lic_info).alias('lic_type').cast('string'),
                check_column_exists('СвЛицензия__ДатаЛиц', lic_info).alias('lic_date').cast('string'),
                check_column_exists('СвЛицензия__ДатаНачЛиц', lic_info).alias('lic_start_date').cast('string'),
                check_column_exists('СвЛицензия__ДатаОкончЛиц', lic_info).alias('lic_end_date').cast('string'),
                check_column_exists('СвЛицензия_НаимЛицВидДеят', lic_info).alias('lic_dotype').cast('string'),
                check_column_exists('СвЛицензия_МестоДейстЛиц', lic_info).alias('lic_place').cast('string'),
                check_column_exists('СвЛицензия_ЛицОргВыдЛиц', lic_info).alias('lic_porg').cast('string'),
                check_column_exists('СвЛицензия_ГРНИПДата__ГРНИП', lic_info).alias('egrip_num').cast('string'),
                check_column_exists('СвЛицензия_ГРНИПДата__ДатаЗаписи', lic_info).alias('egrip_date').cast('string'),
                check_column_exists('СвЛицензия_ГРНИПДатаИспр__ГРНИП', lic_info).alias('egrip_teh_num').cast('string'),
                check_column_exists('СвЛицензия_ГРНИПДатаИспр__ДатаЗаписи', lic_info).alias('egrip_teh_date').cast(
                    'string'),
                check_column_exists('СвЛицензия_СвПриостЛиц__ДатаПриостЛиц', lic_info).alias('lic_stop_date').cast(
                    'string'),
                check_column_exists('СвЛицензия_СвПриостЛиц__ЛицОргПриостЛиц', lic_info).alias('lic_stop_org').cast(
                    'string'),
                check_column_exists('СвЛицензия_СвПриостЛиц_ГРНИПДата__ГРНИП', lic_info).alias('egrip_dop_num').cast(
                    'string'),
                check_column_exists('СвЛицензия_СвПриостЛиц_ГРНИПДата__ДатаЗаписи', lic_info).alias(
                    'egrip_dop_date').cast('string'),
                check_column_exists('СвЛицензия_СвПриостЛиц_ГРНИПДатаИспр__ГРНИП', lic_info).alias(
                    'egrip_teh_dop_num').cast('string'),
                check_column_exists('СвЛицензия_СвПриостЛиц_ГРНИПДатаИспр__ДатаЗаписи', lic_info).alias(
                    'egrip_teh_dop_date').cast('string')]

    lic_info = lic_info.select(*cols).withColumn('table_guid', f.expr("uuid()"))
    return lic_info


def create_noteegrip_info(df):
    noteegrip_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвЗапЕГРИП')))

    noteegrip_info = noteegrip_info.select(f.col('table_guid').alias('rodtable_guid'),
                                           f.col('load_date'),
                                           f.col('folder_date'),
                                           check_column_exists('_СвЗапЕГРИП__ИдЗап', noteegrip_info).alias(
                                               'note_id').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП__ГРНИП', noteegrip_info).alias(
                                               'egrip_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП__ДатаЗап', noteegrip_info).alias(
                                               'egrip_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ВидЗап__КодСПВЗ', noteegrip_info).alias(
                                               'spvz_code').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ВидЗап__НаимВидЗап', noteegrip_info).alias(
                                               'spvz_type').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвРегОрг__КодНО', noteegrip_info).alias(
                                               'no_code').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвРегОрг__НаимНО', noteegrip_info).alias(
                                               'no_name').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СведПредДок_НаимДок', noteegrip_info).alias(
                                               'doc_name').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СведПредДок_НомДок', noteegrip_info).alias(
                                               'doc_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СведПредДок_ДатаДок', noteegrip_info).alias(
                                               'doc_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСвид__Серия', noteegrip_info).alias(
                                               'series').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСвид__Номер', noteegrip_info).alias(
                                               'number').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСвид__ДатаВыдСвид', noteegrip_info).alias(
                                               'cert_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_CвСвид_ГРНИПДатаСвидНед__ГРНИП',
                                                               noteegrip_info).alias('egrip_dop_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_CвСвид_ГРНИПДатаСвидНед__ДатаЗаписи',
                                                               noteegrip_info).alias('egrip_dop_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ГРНИПДатаИспрПред__ИдЗап',
                                                               noteegrip_info).alias('note2_id').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ГРНИПДатаИспрПред__ГРНИП',
                                                               noteegrip_info).alias('egrip2_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ГРНИПДатаИспрПред__ДатаЗап',
                                                               noteegrip_info).alias('egrip2_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ГРНИПДатаНедПред__ИдЗап',
                                                               noteegrip_info).alias('note3_id').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ГРНИПДатаНедПред__ГРНИП',
                                                               noteegrip_info).alias('egrip3_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_ГРНИПДатаНедПред__ДатаЗап',
                                                               noteegrip_info).alias('egrip3_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСтатусЗап_ГРНИПДатаНед__ИдЗап',
                                                               noteegrip_info).alias('note4_id').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСтатусЗап_ГРНИПДатаНед__ГРНИП',
                                                               noteegrip_info).alias('egrip4_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСтатусЗап_ГРНИПДатаНед__ДатаЗап',
                                                               noteegrip_info).alias('egrip4_date').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСтатусЗап_ГРНИПДатаИспр__ИдЗап',
                                                               noteegrip_info).alias('note5_id').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСтатусЗап_ГРНИПДатаИспр__ГРНИП',
                                                               noteegrip_info).alias('egrip5_num').cast('string'),
                                           check_column_exists('_СвЗапЕГРИП_СвСтатусЗап_ГРНИПДатаИспр__ДатаЗап',
                                                               noteegrip_info).alias('egrip5_date').cast('string'),
                                           ).withColumn('table_guid', f.expr("uuid()"))

    return noteegrip_info


def create_fiozags_info(df):
    fiozags_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвФИОЗАГС.*', df)))

    fiozags_info = fiozags_info.select(f.col('table_guid').alias('rodtable_guid'),
                                       f.col('load_date'),
                                       f.col('folder_date'),
                                       check_column_exists('_Фамилия', fiozags_info).alias('surname_zags').cast(
                                           'string'),
                                       check_column_exists('_Имя', fiozags_info).alias('name_zags').cast('string'),
                                       check_column_exists('_Отчество', fiozags_info).alias('patronymic_zags').cast(
                                           'string'),
                                       check_column_exists('ГРНИПДата__ГРНИП', fiozags_info).alias('egrip_num').cast(
                                           'string'),
                                       check_column_exists('ГРНИПДата__ДатаЗаписи', fiozags_info).alias(
                                           'egrip_date').cast('string'),
                                       check_column_exists('ГРНИПДатаИспр__ГРНИП', fiozags_info).alias(
                                           'egrip_teh_num').cast('string'),
                                       check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', fiozags_info).alias(
                                           'egrip_teh_date').cast('string')
                                       ).withColumn('table_guid', f.expr("uuid()"))

    return fiozags_info


def create_ul_info(df):
    ul_info = df.select(f.col('_ДатаВып').alias('ul_date').cast('string'),
                       f.col('_ОГРН').alias('ogrn').cast('string'),
                       f.col('_ДатаОГРН').alias('ogrn_date').cast('string'),
                       f.col('_ИНН').alias('inn').cast('string'),
                       f.col('_КПП').alias('kpp').cast('string'),
                       f.col('_СпрОПФ').alias('opf_info').cast('string'),
                       f.col('_КодОПФ').alias('opf_code').cast('string'),
                       f.col('_ПолнНаимОПФ').alias('opf_name').cast('string'),
                       f.col('table_guid'),
                       f.col('file_name'),
                       f.col('file_date'),
                       f.col('load_date'),
                       f.col('folder_date'))
    return ul_info


def create_ul_name(df):
    ul_name = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвНаимЮЛ.*')))

    if 'СвНаимЮЛКодОКИН' in get_arrays(df.select(check_column_exists('СвНаимЮЛ.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                check_column_exists('_НаимЮЛПолн', ul_name).alias('ulfull_name').cast('string'),
                check_column_exists('_НаимЮЛСокр', ul_name).alias('ulshort_name').cast('string'),
                check_column_exists('ГРНДата__ГРН', ul_name).alias('egrul_num_name').cast('string'),
                check_column_exists('ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_name').cast('string'),
                check_column_exists('ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_name').cast('string'),
                check_column_exists('ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр__НаимСокр', ul_name).alias('short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДата__ГРН', ul_name).alias('egrul_num_short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_short_name').cast('string'),
                check_column_exists('_СвНаимЮЛКодОКИН__КодОКИН', ul_name).alias('lang_code_okin').cast('string'),
                check_column_exists('_СвНаимЮЛКодОКИН__НаимОКИН', ul_name).alias('lang_name_okin').cast('string'),
                check_column_exists('_СвНаимЮЛКодОКИН_ГРНДата__ГРН', ul_name).alias('egrul_num_okin').cast('string'),
                check_column_exists('_СвНаимЮЛКодОКИН_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_okin').cast('string'),
                check_column_exists('_СвНаимЮЛКодОКИН_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_okin').cast('string'),
                check_column_exists('_СвНаимЮЛКодОКИН_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_okin').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн__НаимПолн', ul_name).alias('ulfull_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДата__ГРН', ul_name).alias('egrul_num_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн__НаимСокр', ul_name).alias('ulshort_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДата__ГРН', ul_name).alias('egrul_num_short_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_short_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_short_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_short_name_en').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                check_column_exists('_НаимЮЛПолн', ul_name).alias('ulfull_name').cast('string'),
                check_column_exists('_НаимЮЛСокр', ul_name).alias('ulshort_name').cast('string'),
                check_column_exists('ГРНДата__ГРН', ul_name).alias('egrul_num_name').cast('string'),
                check_column_exists('ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_name').cast('string'),
                check_column_exists('ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_name').cast('string'),
                check_column_exists('ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр__НаимСокр', ul_name).alias('short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДата__ГРН', ul_name).alias('egrul_num_short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_short_name').cast('string'),
                check_column_exists('СвНаимЮЛСокр_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_short_name').cast('string'),
                check_column_exists('СвНаимЮЛКодОКИН__КодОКИН', ul_name).alias('lang_code_okin').cast('string'),
                check_column_exists('СвНаимЮЛКодОКИН__НаимОКИН', ul_name).alias('lang_name_okin').cast('string'),
                check_column_exists('СвНаимЮЛКодОКИН_ГРНДата__ГРН', ul_name).alias('egrul_num_okin').cast('string'),
                check_column_exists('СвНаимЮЛКодОКИН_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_okin').cast('string'),
                check_column_exists('СвНаимЮЛКодОКИН_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_okin').cast('string'),
                check_column_exists('СвНаимЮЛКодОКИН_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_okin').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн__НаимПолн', ul_name).alias('ulfull_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДата__ГРН', ul_name).alias('egrul_num_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_full_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн__НаимСокр', ul_name).alias('ulshort_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДата__ГРН', ul_name).alias('egrul_num_short_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДата__ДатаЗаписи', ul_name).alias('egrul_date_short_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДатаИспр__ГРН', ul_name).alias('egrul_teh_num_short_name_en').cast('string'),
                check_column_exists('СвНаимЮЛСокрИн_ГРНДатаИспр__ДатаЗаписи', ul_name).alias('egrul_teh_date_short_name_en').cast('string')]

    ul_name = ul_name.select(*cols).withColumn('table_guid', f.expr("uuid()")) \
        .withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', '')) \
        .withColumn('short_name', f.regexp_replace(f.col('short_name'), '[\n\r\t]', '')) \
        .withColumn('ulfull_name_en', f.regexp_replace(f.col('ulfull_name_en'), '[\n\r\t]', ''))
    return ul_name


def create_ul_adress(df):
    ul_adress = spark_read(df.select(
            f.col('table_guid'),
            f.col('load_date'),
            f.col('folder_date'),
            f.col('СвАдресЮЛ.*')))

    ul_adress = ul_adress.select(f.col('table_guid').alias('rodtable_guid'),
                    f.col('load_date'),
                    f.col('folder_date'),
                    check_column_exists('АдресРФ__Индекс', ul_adress).cast('string').alias('index'),
                    check_column_exists('АдресРФ__КодРегион', ul_adress).cast('string').alias('region_code'),
                    check_column_exists('АдресРФ__КодАдрКладр', ul_adress).cast('string').alias('cladr_code'),
                    check_column_exists('АдресРФ__Дом', ul_adress).cast('string').alias('home'),
                    check_column_exists('АдресРФ__Корпус', ul_adress).cast('string').alias('corps'),
                    check_column_exists('АдресРФ__Кварт', ul_adress).cast('string').alias('kvart'),
                    check_column_exists('АдресРФ_Регион__ТипРегион', ul_adress).cast('string').alias('region_type'),
                    check_column_exists('АдресРФ_Регион__НаимРегион', ul_adress).cast('string').alias('region_name'),
                    check_column_exists('АдресРФ_Район__ТипРайон', ul_adress).cast('string').alias('district_type'),
                    check_column_exists('АдресРФ_Район__НаимРайон', ul_adress).cast('string').alias('district_name'),
                    check_column_exists('АдресРФ_Город__ТипГород', ul_adress).cast('string').alias('city_type'),
                    check_column_exists('АдресРФ_Город__НаимГород', ul_adress).cast('string').alias('city_name'),
                    check_column_exists('АдресРФ_НаселПункт__ТипНаселПункт', ul_adress).cast('string').alias('locality_type'),
                    check_column_exists('АдресРФ_НаселПункт__НаимНаселПункт', ul_adress).cast('string').alias('locality_name'),
                    check_column_exists('АдресРФ_Улица__ТипУлица', ul_adress).cast('string').alias('street_type'),
                    check_column_exists('АдресРФ_Улица__НаимУлица', ul_adress).cast('string').alias('street_name'),
                    check_column_exists('АдресРФ_ГРНДата__ГРН', ul_adress).cast('string').alias('egrul_num'),
                    check_column_exists('АдресРФ_ГРНДата__ДатаЗаписи', ul_adress).cast('string').alias('egrul_date'),
                    check_column_exists('АдресРФ_ГРНДатаИспр__ГРН', ul_adress).cast('string').alias('egrul_teh_num'),
                    check_column_exists('АдресРФ_ГРНДатаИспр__ДатаЗаписи', ul_adress).cast('string').alias('egrul_teh_date'),
                    check_column_exists('СведОтсутАдресЮЛ__ПризнОтсутАдресЮЛ', ul_adress).cast('string').alias('ul_no_address_type'),
                    check_column_exists('СведОтсутАдресЮЛ_РешСудНедАдр__НаимСуда', ul_adress).cast('string').alias('trial_name'),
                    check_column_exists('СведОтсутАдресЮЛ_РешСудНедАдр__Номер', ul_adress).cast('string').alias('number'),
                    check_column_exists('СведОтсутАдресЮЛ_РешСудНедАдр__Дата', ul_adress).cast('string').alias('solution_date'),
                    check_column_exists('СведОтсутАдресЮЛ_ГРНДата__ГРН', ul_adress).cast('string').alias('egrul_dop_num'),
                    check_column_exists('СведОтсутАдресЮЛ_ГРНДата__ДатаЗаписи', ul_adress).cast('string').alias('egrul_dop_date'),
                    check_column_exists('СведОтсутАдресЮЛ_ГРНДатаИспр__ГРН', ul_adress).cast('string').alias('egrul_teh_dop_num'),
                    check_column_exists('СведОтсутАдресЮЛ_ГРНДатаИспр__ДатаЗаписи', ul_adress).cast('string').alias('egrul_teh_dop_date'),
                    check_column_exists('СвНедАдресЮЛ__ПризнНедАдресЮЛ', ul_adress).cast('string').alias('priz_false_adr'),
                    check_column_exists('СвНедАдресЮЛ__ТекстНедАдресЮЛ', ul_adress).cast('string').alias('text_false_adr'),
                    check_column_exists('СвНедАдресЮЛ_ГРНДата__ГРН', ul_adress).cast('string').alias('egrul_num_false_adr'),
                    check_column_exists('СвНедАдресЮЛ_ГРНДата__ДатаЗаписи', ul_adress).cast('string').alias('egrul_date_false_adr'),
                    check_column_exists('СвНедАдресЮЛ_ГРНДатаИспр__ГРН', ul_adress).cast('string').alias('egrul_teh_num_false_adr'),
                    check_column_exists('СвНедАдресЮЛ_ГРНДатаИспр__ДатаЗаписи', ul_adress).cast('string').alias('egrul_teh_date_false_adr'),
                    check_column_exists('СвНедАдресЮЛ__Дата', ul_adress).cast('string').alias('date_decision_false_adr'),
                    check_column_exists('СвНедАдресЮЛ__НаимСуда', ul_adress).cast('string').alias('jud_name_false_adr'),
                    check_column_exists('СвНедАдресЮЛ__Номер', ul_adress).cast('string').alias('num_decision_false_adr'),
                    check_column_exists('СвРешИзмМН__ТекстРешИзмМН', ul_adress).cast('string').alias('text_adr_ch'),
                    check_column_exists('СвРешИзмМН_Город__НаимГород', ul_adress).cast('string').alias('city_name_ch'),
                    check_column_exists('СвРешИзмМН_Город__ТипГород', ul_adress).cast('string').alias('city_type_ch'),
                    check_column_exists('СвРешИзмМН_ГРНДата__ГРН', ul_adress).cast('string').alias('egrul_num_adr_ch'),
                    check_column_exists('СвРешИзмМН_ГРНДата__ДатаЗаписи', ul_adress).cast('string').alias('egrul_date_adr_ch'),
                    check_column_exists('СвРешИзмМН_ГРНДатаИспр__ГРН', ul_adress).cast('string').alias('egrul_teh_num_adr_ch'),
                    check_column_exists('СвРешИзмМН_ГРНДатаИспр__ДатаЗаписи', ul_adress).cast('string').alias('egrul_teh_date_adr_ch'),
                    check_column_exists('СвРешИзмМН_НаселПункт__НаимНаселПункт', ul_adress).cast('string').alias('locality_name_ch'),
                    check_column_exists('СвРешИзмМН_НаселПункт__ТипНаселПункт', ul_adress).cast('string').alias('locality_type_ch'),
                    check_column_exists('СвРешИзмМН_Район__НаимРайон', ul_adress).cast('string').alias('district_name_ch'),
                    check_column_exists('СвРешИзмМН_Район__ТипРайон', ul_adress).cast('string').alias('district_type_ch'),
                    check_column_exists('СвРешИзмМН_Регион__НаимРегион', ul_adress).cast('string').alias('region_name_ch'),
                    check_column_exists('СвРешИзмМН_Регион__ТипРегион', ul_adress).cast('string').alias('region_type_ch'),
                    check_column_exists('СвМНЮЛ__ИдНом', ul_adress).cast('string').alias('id_gar_mn'),
                    check_column_exists('СвМНЮЛ_Регион', ul_adress).cast('string').alias('region_code_mn'),
                    check_column_exists('СвМНЮЛ_НаимРегион', ul_adress).cast('string').alias('region_name_mn'),
                    check_column_exists('СвМНЮЛ_МуниципРайон__ВидКод', ul_adress).cast('string').alias('municip_type_mn'),
                    check_column_exists('СвМНЮЛ_МуниципРайон__Наим', ul_adress).cast('string').alias('municip_name_mn'),
                    check_column_exists('СвМНЮЛ_ГородСелПоселен__ВидКод', ul_adress).cast('string').alias('posel_type_mn'),
                    check_column_exists('СвМНЮЛ_ГородСелПоселен__Наим', ul_adress).cast('string').alias('posel_name_mn'),
                    check_column_exists('СвМНЮЛ_НаселенПункт__Вид', ul_adress).cast('string').alias('city_type_mn'),
                    check_column_exists('СвМНЮЛ_НаселенПункт__Наим', ul_adress).cast('string').alias('city_name_mn'),
                    check_column_exists('СвМНЮЛ_ГРНДата__ГРН', ul_adress).cast('string').alias('egrul_num_mn'),
                    check_column_exists('СвМНЮЛ_ГРНДата__ДатаЗаписи', ul_adress).cast('string').alias('egrul_date_mn'),
                    check_column_exists('СвМНЮЛ_ГРНДатаИспр__ГРН', ul_adress).cast('string').alias('egrul_teh_num_mn'),
                    check_column_exists('СвМНЮЛ_ГРНДатаИспр__ДатаЗаписи', ul_adress).cast('string').alias('egrul_teh_date_mn'),
                    check_column_exists('СвАдрЮЛФИАС__ИдНом', ul_adress).cast('string').alias('id_gar_fias'),
                    check_column_exists('СвАдрЮЛФИАС__Индекс', ul_adress).cast('string').alias('index_fias'),
                    check_column_exists('СвАдрЮЛФИАС_Регион', ul_adress).cast('string').alias('region_code_fias'),
                    check_column_exists('СвАдрЮЛФИАС_НаимРегион', ul_adress).cast('string').alias('region_name_fias'),
                    check_column_exists('СвАдрЮЛФИАС_МуниципРайон__ВидКод', ul_adress).cast('string').alias('municip_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_МуниципРайон__Наим', ul_adress).cast('string').alias('municip_name_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ГородСелПоселен__ВидКод', ul_adress).cast('string').alias('posel_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ГородСелПоселен__Наим', ul_adress).cast('string').alias('posel_name_fias'),
                    check_column_exists('СвАдрЮЛФИАС_НаселенПункт__Вид', ul_adress).cast('string').alias('city_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_НаселенПункт__Наим', ul_adress).cast('string').alias('city_name_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ЭлПланСтруктур__Тип', ul_adress).cast('string').alias('elplan_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ЭлПланСтруктур__Наим', ul_adress).cast('string').alias('elplan_name_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ЭлУлДорСети__Тип', ul_adress).cast('string').alias('eldor_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ЭлУлДорСети__Наим', ul_adress).cast('string').alias('eldor_name_fias'),
                    check_column_exists('_СвАдрЮЛФИАС_Здание__Тип', ul_adress).cast('string').alias('build_type_fias'),
                    check_column_exists('_СвАдрЮЛФИАС_Здание__Номер', ul_adress).cast('string').alias('build_num_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ПомещЗдания__Тип', ul_adress).cast('string').alias('pom_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ПомещЗдания__Номер', ul_adress).cast('string').alias('pom_num_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ПомещКвартиры__Тип', ul_adress).cast('string').alias('room_type_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ПомещКвартиры__Номер', ul_adress).cast('string').alias('room_num_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ГРНДата__ГРН', ul_adress).cast('string').alias('egrul_num_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ГРНДата__ДатаЗаписи', ul_adress).cast('string').alias('egrul_date_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ГРНДатаИспр__ГРН', ul_adress).cast('string').alias('egrul_teh_num_fias'),
                    check_column_exists('СвАдрЮЛФИАС_ГРНДатаИспр__ДатаЗаписи', ul_adress).cast('string').alias('egrul_teh_date_fias')
                   ).withColumn('table_guid', f.expr("uuid()"))
    return ul_adress


def create_ulemail_info(df):
    ulemail_info = spark_read(df.select(
                f.col('table_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                check_column_exists('СвАдрЭлПочты.*', df)))

    ulemail_info = ulemail_info.select(f.col('table_guid').alias('rodtable_guid'),
                        f.col('load_date'),
                        f.col('folder_date'),
                        check_column_exists('_E-mail', ulemail_info).alias('email').cast('string'),
                        check_column_exists('ГРНДата__ГРН', ulemail_info).alias('egrul_num').cast('string'),
                        check_column_exists('ГРНДата__ДатаЗаписи', ulemail_info).alias('egrul_date').cast('string'),
                        check_column_exists('ГРНДатаИспр__ГРН', ulemail_info).alias('egrul_teh_num').cast('string'),
                        check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulemail_info).alias('egrul_teh_date').cast('string')
                        ).withColumn('table_guid', f.expr("uuid()"))
    return ulemail_info


def create_ulreg_info(df):
    ulreg_info = spark_read(df.select(
                    f.col('table_guid'),
                    f.col('load_date'),
                    f.col('folder_date'),
                    f.col('СвОбрЮЛ.*')))

    ulreg_info = ulreg_info.select(f.col('table_guid').alias('rodtable_guid'),
                            f.col('load_date'),
                            f.col('folder_date'),
                            check_column_exists('_СтатусМКФ', ulreg_info).alias('mkf_stat').cast('string'),
                            check_column_exists('_ОГРН', ulreg_info).alias('ogrn').cast('string'),
                            check_column_exists('_ДатаОГРН', ulreg_info).alias('ogrn_date').cast('string'),
                            check_column_exists('_РегНом', ulreg_info).alias('reg_num').cast('string'),
                            check_column_exists('_ДатаРег', ulreg_info).alias('reg_date').cast('string'),
                            check_column_exists('_НаимРО', ulreg_info).alias('ro_name').cast('string'),
                            check_column_exists('СпОбрЮЛ__КодСпОбрЮЛ', ulreg_info).alias('ulspobr_code').cast('string'),
                            check_column_exists('СпОбрЮЛ__НаимСпОбрЮЛ', ulreg_info).alias('ulspobr_name').cast('string'),
                            check_column_exists('ГРНДата__ГРН', ulreg_info).alias('egrul_num').cast('string'),
                            check_column_exists('ГРНДата__ДатаЗаписи', ulreg_info).alias('egrul_date').cast('string'),
                            check_column_exists('ГРНДатаИспр__ГРН', ulreg_info).alias('egrul_teh_num').cast('string'),
                            check_column_exists('СвРегИнЮЛ__ИННЮЛ', ulreg_info).alias('inul_inn').cast('string'),
                            check_column_exists('СвРегИнЮЛ__НаимЮЛПолнРус', ulreg_info).alias('inul_name_rus').cast('string'),
                            check_column_exists('СвРегИнЮЛ__НаимЮЛПолнЛат', ulreg_info).alias('inul_name_lat').cast('string'),
                            check_column_exists('СвРегИнЮЛ__ОКСМ', ulreg_info).alias('inul_oksm').cast('string'),
                            check_column_exists('СвРегИнЮЛ__НаимСтран', ulreg_info).alias('inul_countr_name').cast('string'),
                            check_column_exists('СвРегИнЮЛ__РегНомер', ulreg_info).alias('inul_reg_num').cast('string'),
                            check_column_exists('СвРегИнЮЛ__КодИОСтрРег', ulreg_info).alias('inul_nal_kode').cast('string'),
                            check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulreg_info).alias('egrul_teh_date').cast('string')
                            ).withColumn('table_guid', f.expr("uuid()"))
    return ulreg_info


def create_ulregorg_info(df):
    ulregorg_info = spark_read(df.select(
                        f.col('table_guid'),
                        f.col('load_date'),
                        f.col('folder_date'),
                        f.col('СвРегОрг.*')))

    ulregorg_info = ulregorg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('_КодНО', ulregorg_info).alias('no_code').cast('string'),
                                check_column_exists('_НаимНО', ulregorg_info).alias('no_name').cast('string'),
                                check_column_exists('_АдрРО', ulregorg_info).alias('ro_address').cast('string'),
                                check_column_exists('ГРНДата__ГРН', ulregorg_info).alias('egrul_num').cast('string'),
                                check_column_exists('ГРНДата__ДатаЗаписи', ulregorg_info).alias('egrul_date').cast('string')
                                ).withColumn('table_guid', f.expr("uuid()"))
    return ulregorg_info


def create_ulstatus_info(df):
    ulstatus_info = spark_read(df.select(
                            f.col('table_guid'),
                            f.col('load_date'),
                            f.col('folder_date'),
                            check_column_exists('СвСтатус.*', df)))

    ulstatus_info = ulstatus_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('ОгрДосСв__ОгрДосСв', ulstatus_info).alias('info_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДата__ГРН', ulstatus_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДата__ДатаЗаписи', ulstatus_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДатаИспр__ГРН', ulstatus_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulstatus_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СвСтатус__КодСтатусЮЛ', ulstatus_info).alias('ulstatus_code').cast('string'),
                                    check_column_exists('СвСтатус__НаимСтатусЮЛ', ulstatus_info).alias('ulstatus_name').cast('string'),
                                    check_column_exists('СвСтатус__СрокЛиквООО', ulstatus_info).alias('liqud_date').cast('string'),
                                    check_column_exists('СвРешИсклЮЛ__ДатаРеш', ulstatus_info).alias('soulution_date').cast('string'),
                                    check_column_exists('СвРешИсклЮЛ__НомерРеш', ulstatus_info).alias('solution_num').cast('string'),
                                    check_column_exists('СвРешИсклЮЛ__ДатаПубликации', ulstatus_info).alias('pub_date').cast('string'),
                                    check_column_exists('СвРешИсклЮЛ__НомерЖурнала', ulstatus_info).alias('journal_num').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulstatus_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulstatus_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ГРН', ulstatus_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulstatus_info).alias('egrul_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))
    return ulstatus_info


def create_ulstop_info(df):
    ulstop_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('СвПрекрЮЛ.*', df)))

    ulstop_info = ulstop_info.select(f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('_ДатаПрекрЮЛ', ulstop_info).alias('ulstop_date').cast('string'),
                                check_column_exists('СпПрекрЮЛ__КодСпПрекрЮЛ', ulstop_info).alias('ulspstop_code').cast('string'),
                                check_column_exists('СпПрекрЮЛ__НаимСпПрекрЮЛ', ulstop_info).alias('ulspstop_name').cast('string'),
                                check_column_exists('СвРегОрг__КодНО', ulstop_info).alias('no_code').cast('string'),
                                check_column_exists('СвРегОрг__НаимНО', ulstop_info).alias('no_name').cast('string'),
                                check_column_exists('ГРНДата__ГРН', ulstop_info).alias('egrul_num').cast('string'),
                                check_column_exists('ГРНДата__ДатаЗаписи', ulstop_info).alias('egrul_date').cast('string')
                                ).withColumn('table_guid', f.expr("uuid()"))
    return ulstop_info


def create_ulrules_info(df):
    ulrules_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('СвТипУстав.*', df)))

    ulrules_info = ulrules_info.select(f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('_НомТипУстав', ulrules_info).alias('rules_num').cast('string'),
                                check_column_exists('ГРНДата__ГРН', ulrules_info).alias('egrul_num_rules').cast('string'),
                                check_column_exists('ГРНДата__ДатаЗаписи', ulrules_info).alias('egrul_date_rules').cast('string'),
                                check_column_exists('ГРНДатаИспр__ГРН', ulrules_info).alias('egrul_teh_num_rules').cast('string'),
                                check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulrules_info).alias('egrul_teh_date_rules').cast('string')
                                ).withColumn('table_guid', f.expr("uuid()"))
    return ulrules_info


def create_ulnouch_info(df):
    ulnouch_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    f.col('СвУчетНО.*')))

    ulnouch_info = ulnouch_info.select(f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('_ИНН', ulnouch_info).alias('inn').cast('string'),
                                check_column_exists('_КПП', ulnouch_info).alias('kpp').cast('string'),
                                check_column_exists('_ДатаПостУч', ulnouch_info).alias('post_uch_date').cast('string'),
                                check_column_exists('СвНО__КодНО', ulnouch_info).alias('no_code').cast('string'),
                                check_column_exists('СвНО__НаимНО', ulnouch_info).alias('no_name').cast('string'),
                                check_column_exists('ГРНДата__ГРН', ulnouch_info).alias('egrul_num').cast('string'),
                                check_column_exists('ГРНДата__ДатаЗаписи', ulnouch_info).alias('egrul_date').cast('string'),
                                check_column_exists('ГРНДатаИспр__ГРН', ulnouch_info).alias('egrul_teh_num').cast('string'),
                                check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulnouch_info).alias('egrul_teh_date').cast('string')
                                ).withColumn('table_guid', f.expr("uuid()"))
    return ulnouch_info


def create_ulpfreg_info(df):
    ulpfreg_info = spark_read(df.select(
                                        f.col('table_guid'),
                                        f.col('load_date'),
                                        f.col('folder_date'),
                                        f.col('СвРегПФ.*')))

    ulpfreg_info = ulpfreg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_РегНомПФ', ulpfreg_info).alias('pfreg_num').cast('string'),
                                    check_column_exists('_ДатаРег', ulpfreg_info).alias('pfreg_date').cast('string'),
                                    check_column_exists('СвОргПФ__КодПФ', ulpfreg_info).alias('pf_code').cast('string'),
                                    check_column_exists('СвОргПФ__НаимПФ', ulpfreg_info).alias('pf_name').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulpfreg_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulpfreg_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ГРН', ulpfreg_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulpfreg_info).alias('egrul_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))
    return ulpfreg_info


def create_ulfssreg_info(df):
    ulfssreg_info = spark_read(df.select(
                        f.col('table_guid'),
                        f.col('load_date'),
                        f.col('folder_date'),
                        f.col('СвРегФСС.*')))

    ulfssreg_info = ulfssreg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_РегНомФСС', ulfssreg_info).alias('fssreg_num').cast('string'),
                                    check_column_exists('_ДатаРег', ulfssreg_info).alias('fssreg_date').cast('string'),
                                    check_column_exists('СвОргФСС__КодФСС', ulfssreg_info).alias('fssreg_code').cast('string'),
                                    check_column_exists('СвОргФСС__НаимФСС', ulfssreg_info).alias('fssreg_name').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulfssreg_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulfssreg_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ГРН', ulfssreg_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulfssreg_info).alias('egrul_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))

    return ulfssreg_info


def create_ulustcap_info(df):
    ulustcap_info = spark_read(df.select(
                            f.col('table_guid'),
                            f.col('load_date'),
                            f.col('folder_date'),
                            f.col('СвУстКап.*')))

    ulustcap_info = ulustcap_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_НаимВидКап', ulustcap_info).alias('capvid_name').cast('string'),
                                    check_column_exists('_СумКап', ulustcap_info).alias('cap_sum').cast('string'),
                                    check_column_exists('ДоляРубля__Числит', ulustcap_info).alias('numerator').cast('string'),
                                    check_column_exists('ДоляРубля__Знаменат', ulustcap_info).alias('denominator').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulustcap_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulustcap_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ГРН', ulustcap_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulustcap_info).alias('egrul_teh_date').cast('string'),
                                    check_column_exists('СведУмУК__ВелУмУК', ulustcap_info).alias('umuk_num').cast('string'),
                                    check_column_exists('СведУмУК__ДатаРеш', ulustcap_info).alias('solution_date').cast('string'),
                                    check_column_exists('СведУмУК_ГРНДата__ГРН', ulustcap_info).alias('egrul_dop_num').cast('string'),
                                    check_column_exists('СведУмУК_ГРНДата__ДатаЗаписи', ulustcap_info).alias('egrul_dop_date').cast('string'),
                                    check_column_exists('СведУмУК_ГРНДатаИспр__ГРН', ulustcap_info).alias('egrul_teh_dop_num').cast('string'),
                                    check_column_exists('СведУмУК_ГРНДатаИспр__ДатаЗаписи', ulustcap_info).alias('egrul_teh_dop_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))

    return ulustcap_info


def create_ulpolnom_info(df):
    ulpolnom_info = spark_read(df.select(
                            f.col('table_guid'),
                            f.col('load_date'),
                            f.col('folder_date'),
                            check_column_exists('СвПолном.*', df)))

    ulpolnom_info = ulpolnom_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_ВидПолном', ulpolnom_info).alias('polnom_type').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulpolnom_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulpolnom_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ГРН', ulpolnom_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulpolnom_info).alias('egrul_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))

    return ulpolnom_info

# EGRUL_2022-08-16_882793.parquet
# uluprorg_info
# 'Can only star expand struct data types. Attribute: `ArrayBuffer(СвУпрОрг)`;'
# ПРОВЕРИТЬ придумать решение
def create_uluprorg_info(df):
    if check_column_exists('СвУпрОрг.*', df)._jc.toString() == 'CAST(NULL AS STRING)':
        uluprorg_info = spark_read(df.select(
            f.col('table_guid'),
            f.col('load_date'),
            f.col('folder_date'),
            check_column_exists('СвУпрОрг', df)))

        uluprorg_info = uluprorg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                             f.col('load_date'),
                                             f.col('folder_date'),
                                             check_column_exists('_СвУпрОрг_ОгрДосСв__ОгрДосСв', uluprorg_info).alias(
                                                 'info_limit').cast('string'),
                                             check_column_exists('_СвУпрОрг_ОгрДосСв_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul_num_limit').cast('string'),
                                             check_column_exists('_СвУпрОрг_ОгрДосСв_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_date_limit').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_ОгрДосСв_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul_teh_num_limit').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_teh_date_limit').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_ГРНДатаПерв__ГРН', uluprorg_info).alias(
                                                 'egrul_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_ГРНДатаПерв__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ__ОГРН', uluprorg_info).alias(
                                                 'ogrn').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ__ИНН', uluprorg_info).alias(
                                                 'inn').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ__НаимЮЛПолн',
                                                                 uluprorg_info).alias('ulfull_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul2_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul2_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul2_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul2_teh_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__ОКСМ', uluprorg_info).alias(
                                                 'oksm').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__НаимСтран', uluprorg_info).alias(
                                                 'county_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__ДатаРег', uluprorg_info).alias(
                                                 'reg_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__РегНомер', uluprorg_info).alias(
                                                 'reg_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__НаимРегОрг', uluprorg_info).alias(
                                                 'regorg_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__КодНПСтрРег', uluprorg_info).alias(
                                                 'np_kode').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн__АдрСтр', uluprorg_info).alias(
                                                 'country_address').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн_ГРНДата__ГРН', uluprorg_info).alias(
                                                 'egrul3_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul3_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul3_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвРегИн_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul3_teh_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг__ПризнНедДанУпрОрг',
                                                                 uluprorg_info).alias('priz_false_ul').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг__ТекстНедДанУпрОрг',
                                                                 uluprorg_info).alias('text_false_ul').cast('string'),
                                             check_column_exists(
                                                 '_СвУпрОрг_СвНедДанУпрОрг_РешСудНедДанУпрОрг__НаимСуда',
                                                 uluprorg_info).alias('jud_name_false_ul').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг_РешСудНедДанУпрОрг__Номер',
                                                                 uluprorg_info).alias('num_decision_false_ul').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг_РешСудНедДанУпрОрг__Дата',
                                                                 uluprorg_info).alias('date_decision_false_ul').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul_num_false_ul').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_date_false_ul').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul_teh_num_false_ul').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвНедДанУпрОрг_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_teh_date_false_ul').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвПредЮЛ__НаимПредЮЛ', uluprorg_info).alias(
                                                 'ulpred_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвПредЮЛ_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul4_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвПредЮЛ_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul4_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвПредЮЛ_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul4_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвПредЮЛ_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul4_teh_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАкРАФП__НомерРАФП', uluprorg_info).alias(
                                                 'rapf_info').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАкРАФП_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul_num_rapf').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАкРАФП_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_date_rapf').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАкРАФП_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul_teh_num_rapf').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвАкРАФП_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul_teh_date_rapf').cast(
                                                 'string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ__Индекс', uluprorg_info).alias(
                                                 'index').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ__КодРегион', uluprorg_info).alias(
                                                 'region_code').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ__КодАдрКладр', uluprorg_info).alias(
                                                 'cladr_code').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ__Дом', uluprorg_info).alias(
                                                 'home').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ__Корпус', uluprorg_info).alias(
                                                 'corps').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ__Кварт', uluprorg_info).alias(
                                                 'kvart').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Город__НаимГород',
                                                                 uluprorg_info).alias('region_type').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Город__ТипГород',
                                                                 uluprorg_info).alias('region_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_НаселПункт__НаимНаселПункт',
                                                                 uluprorg_info).alias('district_type').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_НаселПункт__ТипНаселПункт',
                                                                 uluprorg_info).alias('district_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Район__НаимРайон',
                                                                 uluprorg_info).alias('city_type').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Район__ТипРайон',
                                                                 uluprorg_info).alias('city_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Регион__НаимРегион',
                                                                 uluprorg_info).alias('locality_type').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Регион__ТипРегион',
                                                                 uluprorg_info).alias('locality_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Улица__НаимУлица',
                                                                 uluprorg_info).alias('street_type').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_Улица__ТипУлица',
                                                                 uluprorg_info).alias('street_name').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_ГРНДата__ГРН', uluprorg_info).alias(
                                                 'egrul5_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul5_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul5_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвАдрРФ_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul5_teh_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНомТел__НомТел', uluprorg_info).alias(
                                                 'tel_number').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНомТел_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul6_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНомТел_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul6_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНомТел_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul6_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_СвНомТел_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul6_teh_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_ГРНДатаПерв__ГРН',
                                                                 uluprorg_info).alias('egrul7_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_ГРНДатаПерв__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul7_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ__Фамилия',
                                                                 uluprorg_info).alias('surname').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ__Имя', uluprorg_info).alias(
                                                 'name').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ__Отчество',
                                                                 uluprorg_info).alias('panronymic').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ__ИННФЛ', uluprorg_info).alias(
                                                 'innfl').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul8_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul8_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul8_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul8_teh_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвНомТел__НомТел',
                                                                 uluprorg_info).alias('tel2_number').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвНомТел_ГРНДата__ГРН',
                                                                 uluprorg_info).alias('egrul9_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвНомТел_ГРНДата__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul9_date').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвНомТел_ГРНДатаИспр__ГРН',
                                                                 uluprorg_info).alias('egrul9_teh_num').cast('string'),
                                             check_column_exists('_СвУпрОрг_ПредИнЮЛ_СвНомТел_ГРНДатаИспр__ДатаЗаписи',
                                                                 uluprorg_info).alias('egrul9_teh_date').cast('string')
                                             ).withColumn('table_guid', f.expr("uuid()")).withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', ''))
    else:
        uluprorg_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    f.col('СвУпрОрг.*')))

        uluprorg_info = uluprorg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                        f.col('load_date'),
                                        f.col('folder_date'),
                                        check_column_exists('ОгрДосСв__ОгрДосСв', uluprorg_info).alias('info_limit').cast('string'),
                                        check_column_exists('ОгрДосСв_ГРНДата__ГРН', uluprorg_info).alias('egrul_num_limit').cast('string'),
                                        check_column_exists('ОгрДосСв_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul_date_limit').cast('string'),
                                        check_column_exists('ОгрДосСв_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul_teh_num_limit').cast('string'),
                                        check_column_exists('ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul_teh_date_limit').cast('string'),
                                        check_column_exists('ГРНДатаПерв__ГРН', uluprorg_info).alias('egrul_num').cast('string'),
                                        check_column_exists('ГРНДатаПерв__ДатаЗаписи', uluprorg_info).alias('egrul_date').cast('string'),
                                        check_column_exists('НаимИННЮЛ__ОГРН', uluprorg_info).alias('ogrn').cast('string'),
                                        check_column_exists('НаимИННЮЛ__ИНН', uluprorg_info).alias('inn').cast('string'),
                                        check_column_exists('НаимИННЮЛ__НаимЮЛПолн', uluprorg_info).alias('ulfull_name').cast('string'),
                                        check_column_exists('НаимИННЮЛ_ГРНДата__ГРН', uluprorg_info).alias('egrul2_num').cast('string'),
                                        check_column_exists('НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul2_date').cast('string'),
                                        check_column_exists('НаимИННЮЛ_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul2_teh_num').cast('string'),
                                        check_column_exists('НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul2_teh_date').cast('string'),
                                        check_column_exists('СвРегИн__ОКСМ', uluprorg_info).alias('oksm').cast('string'),
                                        check_column_exists('СвРегИн__НаимСтран', uluprorg_info).alias('county_name').cast('string'),
                                        check_column_exists('СвРегИн__ДатаРег', uluprorg_info).alias('reg_date').cast('string'),
                                        check_column_exists('СвРегИн__РегНомер', uluprorg_info).alias('reg_num').cast('string'),
                                        check_column_exists('СвРегИн__НаимРегОрг', uluprorg_info).alias('regorg_name').cast('string'),
                                        check_column_exists('СвРегИн__КодНПСтрРег', uluprorg_info).alias('np_kode').cast('string'),
                                        check_column_exists('СвРегИн__АдрСтр', uluprorg_info).alias('country_address').cast('string'),
                                        check_column_exists('СвРегИн_ГРНДата__ГРН', uluprorg_info).alias('egrul3_num').cast('string'),
                                        check_column_exists('СвРегИн_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul3_date').cast('string'),
                                        check_column_exists('СвРегИн_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul3_teh_num').cast('string'),
                                        check_column_exists('СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul3_teh_date').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг__ПризнНедДанУпрОрг', uluprorg_info).alias('priz_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг__ТекстНедДанУпрОрг', uluprorg_info).alias('text_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_РешСудНедДанУпрОрг__НаимСуда', uluprorg_info).alias('jud_name_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_РешСудНедДанУпрОрг__Номер', uluprorg_info).alias('num_decision_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_РешСудНедДанУпрОрг__Дата', uluprorg_info).alias('date_decision_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_ГРНДата__ГРН', uluprorg_info).alias('egrul_num_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul_date_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul_teh_num_false_ul').cast('string'),
                                        check_column_exists('СвНедДанУпрОрг_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul_teh_date_false_ul').cast('string'),
                                        check_column_exists('СвПредЮЛ__НаимПредЮЛ', uluprorg_info).alias('ulpred_name').cast('string'),
                                        check_column_exists('СвПредЮЛ_ГРНДата__ГРН', uluprorg_info).alias('egrul4_num').cast('string'),
                                        check_column_exists('СвПредЮЛ_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul4_date').cast('string'),
                                        check_column_exists('СвПредЮЛ_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul4_teh_num').cast('string'),
                                        check_column_exists('СвПредЮЛ_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul4_teh_date').cast('string'),
                                        check_column_exists('СвАкРАФП__НомерРАФП', uluprorg_info).alias('rapf_info').cast('string'),
                                        check_column_exists('СвАкРАФП_ГРНДата__ГРН', uluprorg_info).alias('egrul_num_rapf').cast('string'),
                                        check_column_exists('СвАкРАФП_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul_date_rapf').cast('string'),
                                        check_column_exists('СвАкРАФП_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul_teh_num_rapf').cast('string'),
                                        check_column_exists('СвАкРАФП_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul_teh_date_rapf').cast('string'),
                                        check_column_exists('СвАдрРФ__Индекс', uluprorg_info).alias('index').cast('string'),
                                        check_column_exists('СвАдрРФ__КодРегион', uluprorg_info).alias('region_code').cast('string'),
                                        check_column_exists('СвАдрРФ__КодАдрКладр', uluprorg_info).alias('cladr_code').cast('string'),
                                        check_column_exists('СвАдрРФ__Дом', uluprorg_info).alias('home').cast('string'),
                                        check_column_exists('СвАдрРФ__Корпус', uluprorg_info).alias('corps').cast('string'),
                                        check_column_exists('СвАдрРФ__Кварт', uluprorg_info).alias('kvart').cast('string'),
                                        check_column_exists('СвАдрРФ_Город__НаимГород', uluprorg_info).alias('region_type').cast('string'),
                                        check_column_exists('СвАдрРФ_Город__ТипГород', uluprorg_info).alias('region_name').cast('string'),
                                        check_column_exists('СвАдрРФ_НаселПункт__НаимНаселПункт', uluprorg_info).alias('district_type').cast('string'),
                                        check_column_exists('СвАдрРФ_НаселПункт__ТипНаселПункт', uluprorg_info).alias('district_name').cast('string'),
                                        check_column_exists('СвАдрРФ_Район__НаимРайон', uluprorg_info).alias('city_type').cast('string'),
                                        check_column_exists('СвАдрРФ_Район__ТипРайон', uluprorg_info).alias('city_name').cast('string'),
                                        check_column_exists('СвАдрРФ_Регион__НаимРегион', uluprorg_info).alias('locality_type').cast('string'),
                                        check_column_exists('СвАдрРФ_Регион__ТипРегион', uluprorg_info).alias('locality_name').cast('string'),
                                        check_column_exists('СвАдрРФ_Улица__НаимУлица', uluprorg_info).alias('street_type').cast('string'),
                                        check_column_exists('СвАдрРФ_Улица__ТипУлица', uluprorg_info).alias('street_name').cast('string'),
                                        check_column_exists('СвАдрРФ_ГРНДата__ГРН', uluprorg_info).alias('egrul5_num').cast('string'),
                                        check_column_exists('СвАдрРФ_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul5_date').cast('string'),
                                        check_column_exists('СвАдрРФ_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul5_teh_num').cast('string'),
                                        check_column_exists('СвАдрРФ_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul5_teh_date').cast('string'),
                                        check_column_exists('СвНомТел__НомТел', uluprorg_info).alias('tel_number').cast('string'),
                                        check_column_exists('СвНомТел_ГРНДата__ГРН', uluprorg_info).alias('egrul6_num').cast('string'),
                                        check_column_exists('СвНомТел_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul6_date').cast('string'),
                                        check_column_exists('СвНомТел_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul6_teh_num').cast('string'),
                                        check_column_exists('СвНомТел_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul6_teh_date').cast('string'),
                                        check_column_exists('ПредИнЮЛ_ГРНДатаПерв__ГРН', uluprorg_info).alias('egrul7_num').cast('string'),
                                        check_column_exists('ПредИнЮЛ_ГРНДатаПерв__ДатаЗаписи', uluprorg_info).alias('egrul7_date').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ__Фамилия', uluprorg_info).alias('surname').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ__Имя', uluprorg_info).alias('name').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ__Отчество', uluprorg_info).alias('panronymic').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ__ИННФЛ', uluprorg_info).alias('innfl').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ_ГРНДата__ГРН', uluprorg_info).alias('egrul8_num').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul8_date').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul8_teh_num').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul8_teh_date').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвНомТел__НомТел', uluprorg_info).alias('tel2_number').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвНомТел_ГРНДата__ГРН', uluprorg_info).alias('egrul9_num').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвНомТел_ГРНДата__ДатаЗаписи', uluprorg_info).alias('egrul9_date').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвНомТел_ГРНДатаИспр__ГРН', uluprorg_info).alias('egrul9_teh_num').cast('string'),
                                        check_column_exists('ПредИнЮЛ_СвНомТел_ГРНДатаИспр__ДатаЗаписи', uluprorg_info).alias('egrul9_teh_date').cast('string')
                                        ).withColumn('table_guid', f.expr("uuid()")).withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', ''))
    return uluprorg_info


def create_ul_dover_info(df):
    ul_dover_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                f.col('СведДолжнФЛ')))

    if 'СведДолжнФЛ' in get_arrays(df.select(check_column_exists('СведДолжнФЛ', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_СведДолжнФЛ_ОгрДосСв__ОгрДосСв', ul_dover_info).alias('info_limit').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_ОгрДосСв_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_ОгрДосСв_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_ОгрДосСв_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_ГРНДатаПерв__ГРН', ul_dover_info).alias('egrul_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_ГРНДатаПерв__ДатаЗаписи', ul_dover_info).alias('egrul_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвФЛ__Фамилия', ul_dover_info).alias('surname').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ__Имя', ul_dover_info).alias('name').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ__Отчество', ul_dover_info).alias('panronymic').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ__ИННФЛ', ul_dover_info).alias('innfl').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul2_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul2_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul2_teh_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul2_teh_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС__Фамилия', ul_dover_info).alias('surname_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС__Имя', ul_dover_info).alias('name_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС__Отчество', ul_dover_info).alias('panronymic_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_fio_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_fio_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_fio_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвФИОЗАГС_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_fio_zags').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн__ОГРНИП', ul_dover_info).alias('ogrnip').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн__ВидДолжн', ul_dover_info).alias('dolg_type').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн__НаимВидДолжн', ul_dover_info).alias('dolgtype_name').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн__НаимДолжн', ul_dover_info).alias('dolg_name').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн_ГРНДата__ГРН', ul_dover_info).alias('egrul3_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul3_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul3_teh_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДолжн_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul3_teh_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвНомТел__НомТел', ul_dover_info).alias('tel_number').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвНомТел_ГРНДата__ГРН', ul_dover_info).alias('egrul4_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвНомТел_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul4_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвНомТел_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul4_teh_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвНомТел_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul4_teh_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ__ПризнНедДанДолжнФЛ', ul_dover_info).alias('priz_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ__ТекстНедДанДолжнФЛ', ul_dover_info).alias('text_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_РешСудНедДанДолжнФЛ__НаимСуда', ul_dover_info).alias('jud_name_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_РешСудНедДанДолжнФЛ__Номер', ul_dover_info).alias('num_decision_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_РешСудНедДанДолжнФЛ__Дата', ul_dover_info).alias('date_decision_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_false_dolg').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвПолФЛ__Пол', ul_dover_info).alias('fl_gender').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвПолФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_gender').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_gender').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_gender').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_gender').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ__ДатаРожд', ul_dover_info).alias('birth_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ__МестоРожд', ul_dover_info).alias('birth_place').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ__ПрДатаРожд', ul_dover_info).alias('birth_type').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul5_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul5_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul5_teh_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_CвРождФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul5_teh_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ__КодГражд', ul_dover_info).alias('fl_grazh_type_kode').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ__ОКСМ', ul_dover_info).alias('fl_grazh_countr_kode').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ__НаимСтран', ul_dover_info).alias('fl_grazh_countr_name').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_grazh').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_grazh').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_grazh').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_grazh').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв__ДатаНачДискв', ul_dover_info).alias('disqstart_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв__ДатаОкончДискв', ul_dover_info).alias('disqend_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв__ДатаРеш', ul_dover_info).alias('solution_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв_ГРНДата__ГРН', ul_dover_info).alias('egrul9_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul9_date').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul9_teh_num').cast('string'),
                                    check_column_exists('_СведДолжнФЛ_СвДискв_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul9_teh_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СведДолжнФЛ_ОгрДосСв__ОгрДосСв', ul_dover_info).alias('info_limit').cast('string'),
                                    check_column_exists('СведДолжнФЛ_ОгрДосСв_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('СведДолжнФЛ_ОгрДосСв_ГРНДата__ДатаЗаписи', ul_dover_info).cast('string').alias('egrul_date_limit'),
                                    check_column_exists('СведДолжнФЛ_ОгрДосСв_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('СведДолжнФЛ_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СведДолжнФЛ_ГРНДатаПерв__ГРН', ul_dover_info).alias('egrul_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_ГРНДатаПерв__ДатаЗаписи', ul_dover_info).alias('egrul_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвФЛ__Фамилия', ul_dover_info).alias('surname').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ__Имя', ul_dover_info).alias('name').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ__Отчество', ul_dover_info).alias('panronymic').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ__ИННФЛ', ul_dover_info).alias('innfl').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul2_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul2_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul2_teh_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul2_teh_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС__Фамилия', ul_dover_info).alias('surname_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС__Имя', ul_dover_info).alias('name_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС__Отчество', ul_dover_info).alias('panronymic_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_fio_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_fio_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_fio_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвФИОЗАГС_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_fio_zags').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн__ОГРНИП', ul_dover_info).alias('ogrnip').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн__ВидДолжн', ul_dover_info).alias('dolg_type').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн__НаимВидДолжн', ul_dover_info).alias('dolgtype_name').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн__НаимДолжн', ul_dover_info).alias('dolg_name').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн_ГРНДата__ГРН', ul_dover_info).alias('egrul3_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul3_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul3_teh_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДолжн_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul3_teh_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвНомТел__НомТел', ul_dover_info).alias('tel_number').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвНомТел_ГРНДата__ГРН', ul_dover_info).alias('egrul4_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвНомТел_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul4_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвНомТел_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul4_teh_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвНомТел_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul4_teh_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ__ПризнНедДанДолжнФЛ', ul_dover_info).alias('priz_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ__ТекстНедДанДолжнФЛ', ul_dover_info).alias('text_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_РешСудНедДанДолжнФЛ__НаимСуда', ul_dover_info).alias('jud_name_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_РешСудНедДанДолжнФЛ__Номер', ul_dover_info).alias('num_decision_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_РешСудНедДанДолжнФЛ__Дата', ul_dover_info).alias('date_decision_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвНедДанДолжнФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_false_dolg').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвПолФЛ__Пол', ul_dover_info).alias('fl_gender').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвПолФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_gender').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_gender').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_gender').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_gender').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ__ДатаРожд', ul_dover_info).alias('birth_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ__МестоРожд', ul_dover_info).alias('birth_place').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ__ПрДатаРожд', ul_dover_info).alias('birth_type').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul5_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul5_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul5_teh_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_CвРождФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul5_teh_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ__КодГражд', ul_dover_info).alias('fl_grazh_type_kode').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ__ОКСМ', ul_dover_info).alias('fl_grazh_countr_kode').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ__НаимСтран', ul_dover_info).alias('fl_grazh_countr_name').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ_ГРНДата__ГРН', ul_dover_info).alias('egrul_num_grazh').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul_date_grazh').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul_teh_num_grazh').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul_teh_date_grazh').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв__ДатаНачДискв', ul_dover_info).alias('disqstart_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв__ДатаОкончДискв', ul_dover_info).alias('disqend_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв__ДатаРеш', ul_dover_info).alias('solution_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв_ГРНДата__ГРН', ul_dover_info).alias('egrul9_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв_ГРНДата__ДатаЗаписи', ul_dover_info).alias('egrul9_date').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв_ГРНДатаИспр__ГРН', ul_dover_info).alias('egrul9_teh_num').cast('string'),
                                    check_column_exists('СведДолжнФЛ_СвДискв_ГРНДатаИспр__ДатаЗаписи', ul_dover_info).alias('egrul9_teh_date').cast('string')]

    ul_dover_info = ul_dover_info.select(*cols).withColumn('table_guid', f.expr("uuid()"))
    return ul_dover_info


def create_ul_korp_contr_info(df):
    ul_korp_contr_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('СвКорпДог.*', df)))

    ul_korp_contr_info = ul_korp_contr_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_ВидСведКорпДог', ul_korp_contr_info).alias('contr_info_type').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ul_korp_contr_info).alias('egrul_num_contr').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ul_korp_contr_info).alias('egrul_date_contr').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ГРН', ul_korp_contr_info).alias('egrul_teh_num_contr').cast('string'),
                                    check_column_exists('ГРНДатаИспр__ДатаЗаписи', ul_korp_contr_info).alias('egrul_teh_date_contr').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))
    return ul_korp_contr_info


def create_uluch_ru_info(df):
    uluch_ru_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    f.col('СвУчредит.*')))

    if 'УчрЮЛРос' in get_arrays(df.select(check_column_exists('СвУчредит.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                   f.col('load_date'),
                                   f.col('folder_date'),
                                   check_column_exists('_УчрЮЛРос_ОгрДосСв__ОгрДосСв', uluch_ru_info).alias('uch_ru_info_limit').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ОгрДосСв_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_uch_ru_info').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uch_ru_info').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_uch_ru_info').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_uch_ru_info').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ__ОГРН', uluch_ru_info).alias('ogrn').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ__ИНН', uluch_ru_info).alias('inn').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ__НаимЮЛПолн', uluch_ru_info).alias('ulfull_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul2_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul2_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul2_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul2_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые__РегНом', uluch_ru_info).alias('reg_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые__ДатаРег', uluch_ru_info).alias('reg_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые__НаимРО', uluch_ru_info).alias('ro_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые_ГРНДата__ГРН', uluch_ru_info).alias('egrul3_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul3_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul3_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвРегСтарые_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul3_teh_date').cast('string'),
                                   check_column_exists('__УчрЮЛРос_СвНедДанУчр__ПризнНедДанУчр', uluch_ru_info).alias('priz_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр__ТекстНедДанУчр', uluch_ru_info).alias('text_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_ru_info).alias('jud_name_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_ru_info).alias('num_decision_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_ru_info).alias('date_decision_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_false_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап__НоминСтоим', uluch_ru_info).alias('nomprice').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_ДоляРубля__Числит', uluch_ru_info).alias('numerator_rub').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_ДоляРубля__Знаменат', uluch_ru_info).alias('denominator_rub').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_РазмерДоли_Процент', uluch_ru_info).alias('percent').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_РазмерДоли_ДробДесят', uluch_ru_info).alias('decimals').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_ru_info).alias('nominator').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_ru_info).alias('denominator').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_part').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_part').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_part').cast('string'),
                                   check_column_exists('_УчрЮЛРос_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_part').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбъемПрав__ОбъемПрав', uluch_ru_info).alias('scope_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбъемПрав_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_scope_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_scope_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_scope_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_scope_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем__ВидОбрем', uluch_ru_info).alias('obrem_type').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем__СрокОбременения', uluch_ru_info).alias('obrem_term').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_РешСуд__НаимСуда', uluch_ru_info).alias('judge_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_РешСуд__Номер', uluch_ru_info).alias('judge_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_РешСуд__Дата', uluch_ru_info).alias('dudge_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_ГРНДата__ГРН', uluch_ru_info).alias('egrul5_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul5_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul5_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul5_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul6_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul6_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_ru_info).alias('surname').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_ru_info).alias('name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_ru_info).alias('panronymic').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_ru_info).alias('innfl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul7_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul7_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul7_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul7_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_ru_info).alias('genger_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_genger_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_genger_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_genger_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_genger_uch').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_ru_info).alias('fl_grazh_type_kode').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_ru_info).alias('fl_grazh_countr_kode').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_ru_info).alias('fl_grazh_countr_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_grazh').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_grazh').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_grazh').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_grazh').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_ru_info).alias('zal_number').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_ru_info).alias('zal_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_ru_info).alias('surname2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_ru_info).alias('name2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_ru_info).alias('patronomyc2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__ИННФЛ', uluch_ru_info).alias('innfl2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul13_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul13_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul13_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul13_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul14_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul14_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_ru_info).alias('ogrn2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_ru_info).alias('inn2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_ru_info).alias('ulfull2_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul15_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul15_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul15_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul15_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_ru_info).alias('zalog_in').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_in').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_in').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_in').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_in').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_ru_info).alias('oksm2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимСтран', uluch_ru_info).alias('counry2_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_ru_info).alias('reg2_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_ru_info).alias('reg2_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг',  uluch_ru_info).alias('reg2_name').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_ru_info).alias('np_kode').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_ru_info).alias('reg2_address').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul16_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul16_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul16_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul16_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_ru_info).alias('zal2_number').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_ru_info).alias('zal2_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_ru_info).alias('surname3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_ru_info).alias('name3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_ru_info).alias('patronomyc3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_ru_info).alias('innfl3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul17_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul17_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul17_teh_num').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul17_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал__Номер', uluch_ru_info).alias('zal_number_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал__Дата', uluch_ru_info).alias('zal_date_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_ru_info).alias('surname_not_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_ru_info).alias('name_not_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_ru_info).alias('patronomyc_not_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_ru_info).alias('innfl_not_uprzal').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_uprzal1').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uprzal1').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_uprzal1').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_uprzal1').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал__Номер', uluch_ru_info).alias('zal_number_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал__Дата', uluch_ru_info).alias('zal_date_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_ru_info).alias('surname_not_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_ru_info).alias('name_not_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_ru_info).alias('patronomyc_not_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_ru_info).alias('innfl_not_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_uprzal2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_ru_info).alias('ogrnip_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_ru_info).alias('surname_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_ru_info).alias('name_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_ru_info).alias('patronomyc_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_ru_info).alias('innfl_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_ru_info).alias('ogrn_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_ru_info).alias('inn_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_ru_info).alias('name_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_ru_info).alias('full_name_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_ru_info).alias('oksm_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_ru_info).alias('name_countr_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_ru_info).alias('date_reg_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_ru_info).alias('reg_num_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_ru_info).alias('name_reg_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_ru_info).alias('np_kode_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_ru_info).alias('address_zal_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ__ДатаОткрНасл', uluch_ru_info).alias('date_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр__ОГРН', uluch_ru_info).alias('ogrn_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр__ИНН', uluch_ru_info).alias('inn_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр__НаимЮЛПолн', uluch_ru_info).alias('full_name_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_ul2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_ru_info).alias('full_name_lat_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_ul3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__ОКСМ', uluch_ru_info).alias('oksm_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__НаимСтран', uluch_ru_info).alias('name_countr_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__ДатаРег', uluch_ru_info).alias('date_reg_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__РегНомер', uluch_ru_info).alias('reg_num_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__НаимРегОрг', uluch_ru_info).alias('name_reg_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__КодНПСтрРег', uluch_ru_info).alias('np_kode_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__АдрСтр', uluch_ru_info).alias('address_dov_ul').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_ul4').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul4').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_ul4').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_ul4').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ__ОГРНИП', uluch_ru_info).alias('ogrnip_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ__Фамилия', uluch_ru_info).alias('surname_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ__Имя', uluch_ru_info).alias('name_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ__Отчество', uluch_ru_info).alias('patronomyc_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ__ИННФЛ', uluch_ru_info).alias('innfl_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_fl2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_fl2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_fl2').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ__Пол', uluch_ru_info).alias('genger_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_fl3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_fl3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_fl3').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ__КодГражд', uluch_ru_info).alias('grazh_type_kode_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ__ОКСМ', uluch_ru_info).alias('grazh_countr_kode_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ__НаимСтран', uluch_ru_info).alias('grazh_countr_name_dov_fl').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_fl5').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl5').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_fl5').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_fl5').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                   f.col('load_date'),
                                   f.col('folder_date'),
                                   check_column_exists('УчрЮЛРос_ОгрДосСв__ОгрДосСв', uluch_ru_info).alias('uch_ru_info_limit').cast('string'),
                                   check_column_exists('УчрЮЛРос_ОгрДосСв_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_uch_ru_info').cast('string'),
                                   check_column_exists('УчрЮЛРос_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uch_ru_info').cast('string'),
                                   check_column_exists('УчрЮЛРос_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_uch_ru_info').cast('string'),
                                   check_column_exists('УчрЮЛРос_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_uch_ru_info').cast('string'),
                                   check_column_exists('УчрЮЛРос_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ__ОГРН', uluch_ru_info).alias('ogrn').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ__ИНН', uluch_ru_info).alias('inn').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ__НаимЮЛПолн', uluch_ru_info).alias('ulfull_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul2_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul2_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul2_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul2_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые__РегНом', uluch_ru_info).alias('reg_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые__ДатаРег', uluch_ru_info).alias('reg_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые__НаимРО', uluch_ru_info).alias('ro_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые_ГРНДата__ГРН', uluch_ru_info).alias('egrul3_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul3_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul3_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвРегСтарые_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul3_teh_date').cast('string'),
                                   check_column_exists('_УчрЮЛРос_СвНедДанУчр__ПризнНедДанУчр', uluch_ru_info).alias('priz_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр__ТекстНедДанУчр', uluch_ru_info).alias('text_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_ru_info).alias('jud_name_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_ru_info).alias('num_decision_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_ru_info).alias('date_decision_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_false_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап__НоминСтоим', uluch_ru_info).alias('nomprice').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_ДоляРубля__Числит', uluch_ru_info).alias('numerator_rub').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_ДоляРубля__Знаменат', uluch_ru_info).alias('denominator_rub').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_РазмерДоли_Процент', uluch_ru_info).alias('percent').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_РазмерДоли_ДробДесят', uluch_ru_info).alias('decimals').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_ru_info).alias('nominator').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_ru_info).alias('denominator').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_part').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_part').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_part').cast('string'),
                                   check_column_exists('УчрЮЛРос_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_part').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбъемПрав__ОбъемПрав', uluch_ru_info).alias('scope_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбъемПрав_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_scope_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_scope_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_scope_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_scope_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем__ВидОбрем', uluch_ru_info).alias('obrem_type').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем__СрокОбременения', uluch_ru_info).alias('obrem_term').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_РешСуд__НаимСуда', uluch_ru_info).alias('judge_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_РешСуд__Номер', uluch_ru_info).alias('judge_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_РешСуд__Дата', uluch_ru_info).alias('dudge_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_ГРНДата__ГРН', uluch_ru_info).alias('egrul5_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul5_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul5_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul5_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul6_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul6_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_ru_info).alias('surname').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_ru_info).alias('name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_ru_info).alias('panronymic').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_ru_info).alias('innfl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul7_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul7_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul7_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul7_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_ru_info).alias('genger_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_genger_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_genger_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_genger_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_genger_uch').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_ru_info).alias('fl_grazh_type_kode').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_ru_info).alias('fl_grazh_countr_kode').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_ru_info).alias('fl_grazh_countr_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_grazh').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_grazh').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_grazh').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_grazh').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_ru_info).alias('zal_number').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_ru_info).alias('zal_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_ru_info).alias('surname2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_ru_info).alias('name2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_ru_info).alias('patronomyc2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__ИННФЛ', uluch_ru_info).alias('innfl2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul13_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul13_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul13_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul13_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul14_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul14_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_ru_info).alias('ogrn2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_ru_info).alias('inn2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_ru_info).alias('ulfull2_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul15_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul15_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul15_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul15_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_ru_info).alias('zalog_in').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_in').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_in').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_in').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_in').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_ru_info).alias('oksm2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимСтран', uluch_ru_info).alias('counry2_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_ru_info).alias('reg2_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_ru_info).alias('reg2_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг',  uluch_ru_info).alias('reg2_name').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_ru_info).alias('np_kode').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_ru_info).alias('reg2_address').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul16_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul16_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul16_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul16_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_ru_info).alias('zal2_number').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_ru_info).alias('zal2_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_ru_info).alias('surname3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_ru_info).alias('name3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_ru_info).alias('patronomyc3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_ru_info).alias('innfl3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul17_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul17_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul17_teh_num').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul17_teh_date').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал__Номер', uluch_ru_info).alias('zal_number_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал__Дата', uluch_ru_info).alias('zal_date_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_ru_info).alias('surname_not_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_ru_info).alias('name_not_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_ru_info).alias('patronomyc_not_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_ru_info).alias('innfl_not_uprzal').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_uprzal1').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uprzal1').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_uprzal1').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_uprzal1').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал__Номер', uluch_ru_info).alias('zal_number_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал__Дата', uluch_ru_info).alias('zal_date_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_ru_info).alias('surname_not_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_ru_info).alias('name_not_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_ru_info).alias('patronomyc_not_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_ru_info).alias('innfl_not_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_uprzal2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_ru_info).alias('ogrnip_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_ru_info).alias('surname_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_ru_info).alias('name_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_ru_info).alias('patronomyc_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_ru_info).alias('innfl_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_ru_info).alias('ogrn_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_ru_info).alias('inn_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_ru_info).alias('name_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_ru_info).alias('full_name_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_ru_info).alias('oksm_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_ru_info).alias('name_countr_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_ru_info).alias('date_reg_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_ru_info).alias('reg_num_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_ru_info).alias('name_reg_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_ru_info).alias('np_kode_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_ru_info).alias('address_zal_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_zal_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_zal_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_zal_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ__ДатаОткрНасл', uluch_ru_info).alias('date_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр__ОГРН', uluch_ru_info).alias('ogrn_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр__ИНН', uluch_ru_info).alias('inn_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр__НаимЮЛПолн', uluch_ru_info).alias('full_name_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_ul2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_ru_info).alias('full_name_lat_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_ul3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__ОКСМ', uluch_ru_info).alias('oksm_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__НаимСтран', uluch_ru_info).alias('name_countr_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__ДатаРег', uluch_ru_info).alias('date_reg_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__РегНомер', uluch_ru_info).alias('reg_num_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__НаимРегОрг', uluch_ru_info).alias('name_reg_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__КодНПСтрРег', uluch_ru_info).alias('np_kode_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн__АдрСтр', uluch_ru_info).alias('address_dov_ul').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_ul4').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_ul4').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_ul4').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_ul4').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ__ОГРНИП', uluch_ru_info).alias('ogrnip_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_ГРНДатаПерв__ГРН', uluch_ru_info).alias('egrul_num_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ__Фамилия', uluch_ru_info).alias('surname_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ__Имя', uluch_ru_info).alias('name_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ__Отчество', uluch_ru_info).alias('patronomyc_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ__ИННФЛ', uluch_ru_info).alias('innfl_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_fl2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_fl2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_fl2').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ__Пол', uluch_ru_info).alias('genger_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_fl3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_fl3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_fl3').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ__КодГражд', uluch_ru_info).alias('grazh_type_kode_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ__ОКСМ', uluch_ru_info).alias('grazh_countr_kode_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ__НаимСтран', uluch_ru_info).alias('grazh_countr_name_dov_fl').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_ru_info).alias('egrul_num_dov_fl5').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_ru_info).alias('egrul_date_dov_fl5').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_ru_info).alias('egrul_teh_num_dov_fl5').cast('string'),
                                   check_column_exists('УчрЮЛРос_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_ru_info).alias('egrul_teh_date_dov_fl5').cast('string')]

    uluch_ru_info = uluch_ru_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', '')).withColumn('full_name_dov_ul', f.regexp_replace(f.col('full_name_dov_ul'), '[\n\r\t]', ''))

    return uluch_ru_info


# может быть массивом с _ может быть и нет
def create_uluch_in_info(df):
    uluch_in_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    f.col('СвУчредит.*')))

    if 'УчрЮЛИн' in get_arrays(df.select(check_column_exists('СвУчредит.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                               f.col('load_date'),
                               f.col('folder_date'),
                               check_column_exists('_УчрЮЛИн_ОгрДосСв__ОгрДосСв', uluch_in_info).alias('uch_in_info_limit').cast('string'),
                               check_column_exists('_УчрЮЛИн_ОгрДосСв_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_uch_in_info').cast('string'),
                               check_column_exists('_УчрЮЛИн_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_uch_in_info').cast('string'),
                               check_column_exists('_УчрЮЛИн_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_uch_in_info').cast('string'),
                               check_column_exists('_УчрЮЛИн_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_uch_in_info').cast('string'),
                               check_column_exists('_УчрЮЛИн_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul18_num').cast('string'),
                               check_column_exists('_УчрЮЛИн_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul18_date').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ__ОГРН', uluch_in_info).alias('ogrn3').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ__ИНН', uluch_in_info).alias('inn3').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ__НаимЮЛПолн', uluch_in_info).alias('ulfull3_name').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul19_num').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul19_date').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul19_teh_num').cast('string'),
                               check_column_exists('_УчрЮЛИн_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul19_teh_date').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('full_name_lat').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_name_lat').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_name_lat').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_name_lat').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_name_lat').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн__ОКСМ', uluch_in_info).alias('oksm3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн__ДатаРег', uluch_in_info).alias('reg3_date').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн__РегНомер', uluch_in_info).alias('reg3_num').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн__НаимРегОрг', uluch_in_info).alias('reg3_name').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np_kode').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн__АдрСтр', uluch_in_info).alias('reg3_address').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul20_num').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul20_date').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul20_teh_num').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul20_teh_date').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр__ПризнНедДанУчр', uluch_in_info).alias('priz_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр__ТекстНедДанУчр', uluch_in_info).alias('text_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_in_info).alias('jud_name_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_in_info).alias('num_decision_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_in_info).alias('date_decision_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_false_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап__НоминСтоим', uluch_in_info).alias('nomprice').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_ДоляРубля__Числит', uluch_in_info).alias('numerator_rub').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_ДоляРубля__Знаменат', uluch_in_info).alias('denominator_rub').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_РазмерДоли_Процент', uluch_in_info).alias('percent').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_РазмерДоли__ДробДесят', uluch_in_info).alias('decimals').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_in_info).alias('nominator').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_in_info).alias('denominator').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_part').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_part').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_part').cast('string'),
                               check_column_exists('_УчрЮЛИн_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_part').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбъемПрав__ОбъемПрав', uluch_in_info).alias('scope_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбъемПрав_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_scope_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_scope_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_scope_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_scope_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем__ВидОбрем', uluch_in_info).alias('obrem_typeB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем__СрокОбременения', uluch_in_info).alias('obrem_termB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_РешСуд__НаимСуда', uluch_in_info).alias('judge_nameB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_РешСуд__Номер', uluch_in_info).alias('judge_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_РешСуд__Дата', uluch_in_info).alias('dudge_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_ГРНДата__ГРН', uluch_in_info).alias('egrul5_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul5_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul5_teh_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul5_teh_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul6_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul6_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_in_info).alias('surnameB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_in_info).alias('nameB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_in_info).alias('panronymicB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_in_info).alias('innflB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul7_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul7_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul7_teh_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul7_teh_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_in_info).alias('genger_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_genger_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_genger_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_genger_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_genger_uch').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_in_info).alias('fl_grazh_type_kode').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_in_info).alias('fl_grazh_countr_kode').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_in_info).alias('fl_grazh_countr_name').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_grazh').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_grazh').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_grazh').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_grazh').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_in_info).alias('zal_numberB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_in_info).alias('zal_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_in_info).alias('surname2B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_in_info).alias('name2B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_in_info).alias('patronomyc2B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul13_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul13_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul13_teh_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul13_teh_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul14_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul14_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_in_info).alias('ogrn2B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_in_info).alias('inn2B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_in_info).alias('ulfull2_nameB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul15_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul15_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul15_teh_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul15_teh_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('zalog_in').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_in').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_in').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_in').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_in').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_in_info).alias('oksm2B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_in_info).alias('reg2_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_in_info).alias('reg2_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_in_info).alias('reg2_nameB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np2_kode').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_in_info).alias('reg2_addressB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul16_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul16_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul16_teh_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul16_teh_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_in_info).alias('zal2_numberB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_in_info).alias('zal2_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_in_info).alias('surname3B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_in_info).alias('name3B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_in_info).alias('patronomyc3B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_in_info).alias('innfl3B').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul17_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul17_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul17_teh_numB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul17_teh_dateB').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul_num_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul_date_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал__Номер', uluch_in_info).alias('zal_number_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал__Дата', uluch_in_info).alias('zal_date_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_in_info).alias('surname_not_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_in_info).alias('name_not_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_in_info).alias('patronomyc_not_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_in_info).alias('innfl_not_uprzal').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_uprzal1').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_uprzal1').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_uprzal1').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_uprzal1').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал__Номер', uluch_in_info).alias('zal_number_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал__Дата', uluch_in_info).alias('zal_date_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_in_info).alias('surname_not_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_in_info).alias('name_not_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_in_info).alias('patronomyc_not_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_in_info).alias('innfl_not_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_uprzal2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_in_info).alias('ogrnip_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_in_info).alias('surname_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_in_info).alias('name_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_in_info).alias('patronomyc_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_in_info).alias('innfl_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_in_info).alias('ogrn_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_in_info).alias('inn_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_in_info).alias('name_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('full_name_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_in_info).alias('oksm_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_in_info).alias('name_countr_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_in_info).alias('date_reg_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_in_info).alias('reg_num_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_in_info).alias('name_reg_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np_kode_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_in_info).alias('address_zal_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ__ДатаОткрНасл', uluch_in_info).alias('date_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul_num_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр__ОГРН', uluch_in_info).alias('ogrn_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр__ИНН', uluch_in_info).alias('inn_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр__НаимЮЛПолн', uluch_in_info).alias('full_name_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_ul2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('full_name_lat_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_ul3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__ОКСМ', uluch_in_info).alias('oksm_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__НаимСтран', uluch_in_info).alias('name_countr_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__ДатаРег', uluch_in_info).alias('date_reg_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__РегНомер', uluch_in_info).alias('reg_num_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__НаимРегОрг', uluch_in_info).alias('name_reg_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np_kode_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__АдрСтр', uluch_in_info).alias('address_dov_ul').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_ul4').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul4').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_ul4').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_ul4').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ__ОГРНИП', uluch_in_info).alias('ogrnip_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul_num_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ__Фамилия', uluch_in_info).alias('surname_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ__Имя', uluch_in_info).alias('name_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ__Отчество', uluch_in_info).alias('patronomyc_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ__ИННФЛ', uluch_in_info).alias('innfl_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_fl2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_fl2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_fl2').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ__Пол', uluch_in_info).alias('genger_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_fl3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_fl3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_fl3').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ__КодГражд', uluch_in_info).alias('grazh_type_kode_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ__ОКСМ', uluch_in_info).alias('grazh_countr_kode_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ__НаимСтран', uluch_in_info).alias('grazh_countr_name_dov_fl').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_fl5').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl5').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_fl5').cast('string'),
                               check_column_exists('_УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_fl5').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                               f.col('load_date'),
                               f.col('folder_date'),
                               check_column_exists('УчрЮЛИн_ОгрДосСв__ОгрДосСв', uluch_in_info).alias('uch_in_info_limit').cast('string'),
                               check_column_exists('УчрЮЛИн_ОгрДосСв_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_uch_in_info').cast('string'),
                               check_column_exists('УчрЮЛИн_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_uch_in_info').cast('string'),
                               check_column_exists('УчрЮЛИн_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_uch_in_info').cast('string'),
                               check_column_exists('УчрЮЛИн_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_uch_in_info').cast('string'),
                               check_column_exists('УчрЮЛИн_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul18_num').cast('string'),
                               check_column_exists('УчрЮЛИн_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul18_date').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ__ОГРН', uluch_in_info).alias('ogrn3').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ__ИНН', uluch_in_info).alias('inn3').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ__НаимЮЛПолн', uluch_in_info).alias('ulfull3_name').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul19_num').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul19_date').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul19_teh_num').cast('string'),
                               check_column_exists('УчрЮЛИн_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul19_teh_date').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('full_name_lat').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_name_lat').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_name_lat').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_name_lat').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_name_lat').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн__ОКСМ', uluch_in_info).alias('oksm3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн__ДатаРег', uluch_in_info).alias('reg3_date').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн__РегНомер', uluch_in_info).alias('reg3_num').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн__НаимРегОрг', uluch_in_info).alias('reg3_name').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np_kode').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн__АдрСтр', uluch_in_info).alias('reg3_address').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul20_num').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul20_date').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul20_teh_num').cast('string'),
                               check_column_exists('УчрЮЛИн_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul20_teh_date').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр__ПризнНедДанУчр', uluch_in_info).alias('priz_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр__ТекстНедДанУчр', uluch_in_info).alias('text_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_in_info).alias('jud_name_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_in_info).alias('num_decision_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_in_info).alias('date_decision_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_false_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап__НоминСтоим', uluch_in_info).alias('nomprice').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_ДоляРубля__Числит', uluch_in_info).alias('numerator_rub').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_ДоляРубля__Знаменат', uluch_in_info).alias('denominator_rub').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_РазмерДоли_Процент', uluch_in_info).alias('percent').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_РазмерДоли__ДробДесят', uluch_in_info).alias('decimals').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_in_info).alias('nominator').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_in_info).alias('denominator').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_part').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_part').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_part').cast('string'),
                               check_column_exists('УчрЮЛИн_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_part').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбъемПрав__ОбъемПрав', uluch_in_info).alias('scope_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбъемПрав_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_scope_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_scope_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_scope_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_scope_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем__ВидОбрем', uluch_in_info).alias('obrem_typeB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем__СрокОбременения', uluch_in_info).alias('obrem_termB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_РешСуд__НаимСуда', uluch_in_info).alias('judge_nameB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_РешСуд__Номер', uluch_in_info).alias('judge_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_РешСуд__Дата', uluch_in_info).alias('dudge_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_ГРНДата__ГРН', uluch_in_info).alias('egrul5_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul5_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul5_teh_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul5_teh_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul6_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul6_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_in_info).alias('surnameB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_in_info).alias('nameB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_in_info).alias('panronymicB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_in_info).alias('innflB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul7_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul7_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul7_teh_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul7_teh_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_in_info).alias('genger_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_genger_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_genger_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_genger_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_genger_uch').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_in_info).alias('fl_grazh_type_kode').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_in_info).alias('fl_grazh_countr_kode').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_in_info).alias('fl_grazh_countr_name').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_grazh').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_grazh').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_grazh').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_grazh').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_in_info).alias('zal_numberB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_in_info).alias('zal_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_in_info).alias('surname2B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_in_info).alias('name2B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_in_info).alias('patronomyc2B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul13_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul13_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul13_teh_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul13_teh_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul14_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul14_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_in_info).alias('ogrn2B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_in_info).alias('inn2B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_in_info).alias('ulfull2_nameB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul15_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul15_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul15_teh_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul15_teh_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('zalog_in').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_in').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_in').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_in').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_in').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_in_info).alias('oksm2B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_in_info).alias('reg2_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_in_info).alias('reg2_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_in_info).alias('reg2_nameB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np2_kode').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_in_info).alias('reg2_addressB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul16_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul16_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul16_teh_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul16_teh_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_in_info).alias('zal2_numberB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_in_info).alias('zal2_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_in_info).alias('surname3B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_in_info).alias('name3B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_in_info).alias('patronomyc3B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_in_info).alias('innfl3B').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul17_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul17_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul17_teh_numB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul17_teh_dateB').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul_num_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul_date_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал__Номер', uluch_in_info).alias('zal_number_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал__Дата', uluch_in_info).alias('zal_date_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_in_info).alias('surname_not_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_in_info).alias('name_not_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_in_info).alias('patronomyc_not_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_in_info).alias('innfl_not_uprzal').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_uprzal1').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_uprzal1').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_uprzal1').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_uprzal1').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал__Номер', uluch_in_info).alias('zal_number_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал__Дата', uluch_in_info).alias('zal_date_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_in_info).alias('surname_not_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_in_info).alias('name_not_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_in_info).alias('patronomyc_not_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_in_info).alias('innfl_not_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_uprzal2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_in_info).alias('ogrnip_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_in_info).alias('surname_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_in_info).alias('name_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_in_info).alias('patronomyc_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_in_info).alias('innfl_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_in_info).alias('ogrn_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_in_info).alias('inn_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_in_info).alias('name_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('full_name_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_in_info).alias('oksm_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_in_info).alias('name_countr_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_in_info).alias('date_reg_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_in_info).alias('reg_num_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_in_info).alias('name_reg_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np_kode_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_in_info).alias('address_zal_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_zal_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_zal_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_zal_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ__ДатаОткрНасл', uluch_in_info).alias('date_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul_num_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр__ОГРН', uluch_in_info).alias('ogrn_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр__ИНН', uluch_in_info).alias('inn_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр__НаимЮЛПолн', uluch_in_info).alias('full_name_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_ul2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_in_info).alias('full_name_lat_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_ul3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__ОКСМ', uluch_in_info).alias('oksm_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__НаимСтран', uluch_in_info).alias('name_countr_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__ДатаРег', uluch_in_info).alias('date_reg_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__РегНомер', uluch_in_info).alias('reg_num_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__НаимРегОрг', uluch_in_info).alias('name_reg_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__КодНПСтрРег', uluch_in_info).alias('np_kode_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн__АдрСтр', uluch_in_info).alias('address_dov_ul').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_ul4').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_ul4').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_ul4').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_ul4').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ__ОГРНИП', uluch_in_info).alias('ogrnip_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_ГРНДатаПерв__ГРН', uluch_in_info).alias('egrul_num_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ__Фамилия', uluch_in_info).alias('surname_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ__Имя', uluch_in_info).alias('name_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ__Отчество', uluch_in_info).alias('patronomyc_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ__ИННФЛ', uluch_in_info).alias('innfl_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_fl2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_fl2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_fl2').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ__Пол', uluch_in_info).alias('genger_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_fl3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_fl3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_fl3').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ__КодГражд', uluch_in_info).alias('grazh_type_kode_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ__ОКСМ', uluch_in_info).alias('grazh_countr_kode_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ__НаимСтран', uluch_in_info).alias('grazh_countr_name_dov_fl').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_in_info).alias('egrul_num_dov_fl5').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_in_info).alias('egrul_date_dov_fl5').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_in_info).alias('egrul_teh_num_dov_fl5').cast('string'),
                               check_column_exists('УчрЮЛИн_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_in_info).alias('egrul_teh_date_dov_fl5').cast('string')]

    uluch_in_info = uluch_in_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('full_name_dov_ul', f.regexp_replace(f.col('full_name_dov_ul'), '[\n\r\t]', ''))

    return uluch_in_info


def create_uluch_fl_info(df):
    uluch_fl_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                f.col('СвУчредит.*')))
    cols = [
        f.col('table_guid').alias('rodtable_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('_УчрФЛ__ОГРНИП', uluch_fl_info).alias('ogrnip_uch_fl').cast('string'),
        check_column_exists('_УчрФЛ_ОгрДосСв__ОгрДосСв', uluch_fl_info).alias('uch_fl_info_limit').cast('string'),
        check_column_exists('_УчрФЛ_ОгрДосСв_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_uch_fl_info').cast(
            'string'),
        check_column_exists('_УчрФЛ_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul_date_uch_fl_info').cast(
            'string'),
        check_column_exists('_УчрФЛ_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul_teh_num_uch_fl_info').cast(
            'string'),
        check_column_exists('_УчрФЛ_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_uch_fl_info').cast('string'),
        check_column_exists('_УчрФЛ_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul22_num').cast('string'),
        check_column_exists('_УчрФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias('egruL22_date').cast('string'),
        check_column_exists('_УчрФЛ_СВФл__Фамилия', uluch_fl_info).alias('surname4').cast('string'),
        check_column_exists('_УчрФЛ_СвФЛ__Имя', uluch_fl_info).alias('name4').cast('string'),
        check_column_exists('_УчрФЛ_СвФЛ__Отчество', uluch_fl_info).alias('panronymic4').cast('string'),
        check_column_exists('_УчрФЛ_СвФЛ__ИННФЛ', uluch_fl_info).alias('innfl4').cast('string'),
        check_column_exists('_УчрФЛ_СвФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul23_num').cast('string'),
        check_column_exists('_УчрФЛ_СВФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul23_date').cast('string'),
        check_column_exists('_УчрФЛ_СВФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul23_teh_num').cast('string'),
        check_column_exists('_УчрФЛ_СВФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias('egrul23_teh_date').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС__Фамилия', uluch_fl_info).alias('surnam_zags').cast('string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС__Имя', uluch_fl_info).alias('name_zags').cast('string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС__Отчество', uluch_fl_info).alias('panronymic_zags').cast('string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_zags').cast('string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul_date_zags').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul_teh_num_zags').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвФИОЗАГС_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_zags').cast('string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр__ПризнНедДанУчр', uluch_fl_info).alias('priz_false_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр__ТекстНедДанУчр', uluch_fl_info).alias('text_false_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_fl_info).alias(
            'jud_name_false_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_fl_info).alias(
            'num_decision_false_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_fl_info).alias(
            'date_decision_false_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_false_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul_date_false_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul_teh_num_false_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_false_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвПолФЛ__Пол', uluch_fl_info).alias('genger_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_genger_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul_date_genger_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul_teh_num_genger_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_genger_uch').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап__НоминСтоим', uluch_fl_info).alias('nomprice').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_ДоляРубля__Числит', uluch_fl_info).alias('numerator_rub').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_ДоляРубля__Знаменат', uluch_fl_info).alias('denominator_rub').cast(
            'string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_РазмерДоли__Процент', uluch_fl_info).alias('percent').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_РазмерДоли__ДробДесят', uluch_fl_info).alias('decimals').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_fl_info).alias('nominator').cast(
            'string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_fl_info).alias(
            'denominator').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_part').cast('string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul_date_part').cast(
            'string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul_teh_num_part').cast(
            'string'),
        check_column_exists('_УчрФЛ_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_part').cast('string'),
        check_column_exists('_УчрФЛ_СвОбъемПрав__ОбъемПрав', uluch_fl_info).alias('scope_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвОбъемПрав_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_scope_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul_date_scope_uch').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_scope_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_scope_uch').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем__ВидОбрем', uluch_fl_info).alias('obrem_typeC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем__СрокОбременения', uluch_fl_info).alias('obrem_termC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_РешСуд__НаимСуда', uluch_fl_info).alias('judge_nameC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_РешСуд__Номер', uluch_fl_info).alias('judge_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_РешСуд__Дата', uluch_fl_info).alias('dudge_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_ГРНДата__ГРН', uluch_fl_info).alias('egrul5_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul5_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul5_teh_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias('egrul5_teh_dateC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul6_numC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias(
            'egrul6_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_fl_info).alias('surnameC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_fl_info).alias('nameC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_fl_info).alias('panronymicC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_fl_info).alias('innflC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul7_numC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul7_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul7_teh_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul7_teh_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_fl_info).alias('genger_uch2').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_genger_uch2').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_genger_uch2').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_genger_uch2').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_genger_uch2').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_fl_info).alias(
            'fl_grazh_type_kode').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_fl_info).alias(
            'fl_grazh_countr_kode').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_fl_info).alias(
            'fl_grazh_countr_name').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_grazh').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_grazh').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_grazh').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_grazh').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_fl_info).alias(
            'zal_numberC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_fl_info).alias('zal_dateC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_fl_info).alias(
            'surname2C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_fl_info).alias(
            'name2C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_fl_info).alias(
            'patronomyc2C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul13_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи',
                            uluch_fl_info).alias('egrul13_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН',
                            uluch_fl_info).alias('egrul13_teh_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи',
                            uluch_fl_info).alias('egrul13_teh_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul14_numC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias(
            'egrul14_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_fl_info).alias('ogrn2C').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_fl_info).alias('inn2C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_fl_info).alias(
            'ulfull2_nameC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul15_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul15_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul15_teh_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul15_teh_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_fl_info).alias(
            'zalog_in').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_zal_in').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_zal_in').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_zal_in').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_zal_in').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_fl_info).alias('oksm2C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_fl_info).alias('reg2_dateC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_fl_info).alias('reg2_numC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_fl_info).alias('reg2_nameC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_fl_info).alias('np_kode').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_fl_info).alias('reg2_addressC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul16_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul16_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul16_teh_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul16_teh_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_fl_info).alias(
            'zal2_numberC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_fl_info).alias('zal2_dateC').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_fl_info).alias(
            'surname3C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_fl_info).alias(
            'name3C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_fl_info).alias(
            'patronomyc3C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_fl_info).alias(
            'innfl3C').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul17_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи',
                            uluch_fl_info).alias('egrul17_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul17_teh_numC').cast('string'),
        check_column_exists('_УчрФЛ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                            uluch_fl_info).alias('egrul17_teh_dateC').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul_num_uprzal').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias('egrul_date_uprzal').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал__Номер', uluch_fl_info).alias('zal_number_uprzal').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал__Дата', uluch_fl_info).alias('zal_date_uprzal').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_fl_info).alias(
            'surname_not_uprzal').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_fl_info).alias(
            'name_not_uprzal').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_fl_info).alias(
            'patronomyc_not_uprzal').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_fl_info).alias(
            'innfl_not_uprzal').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_uprzal1').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_uprzal1').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_uprzal1').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_uprzal1').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал__Номер', uluch_fl_info).alias('zal_number_uprzal2').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал__Дата', uluch_fl_info).alias('zal_date_uprzal2').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_fl_info).alias(
            'surname_not_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_fl_info).alias(
            'name_not_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_fl_info).alias(
            'patronomyc_not_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_fl_info).alias(
            'innfl_not_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_uprzal2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_fl_info).alias('ogrnip_zal_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_fl_info).alias('surname_zal_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_fl_info).alias('name_zal_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_fl_info).alias(
            'patronomyc_zal_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_fl_info).alias('innfl_zal_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_zal_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_zal_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_zal_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_zal_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_fl_info).alias('ogrn_zal_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_fl_info).alias('inn_zal_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_fl_info).alias(
            'name_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_fl_info).alias(
            'full_name_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_zal_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_zal_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_zal_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_zal_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_fl_info).alias('oksm_zal_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_fl_info).alias(
            'name_countr_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_fl_info).alias(
            'date_reg_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_fl_info).alias(
            'reg_num_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_fl_info).alias(
            'name_reg_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_fl_info).alias(
            'np_kode_zal_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_fl_info).alias('address_zal_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_zal_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_zal_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_zal_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_zal_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ__ДатаОткрНасл', uluch_fl_info).alias('date_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul_num_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias('egrul_date_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр__ОГРН', uluch_fl_info).alias('ogrn_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр__ИНН', uluch_fl_info).alias('inn_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр__НаимЮЛПолн', uluch_fl_info).alias(
            'full_name_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_dov_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_dov_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_dov_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_НаимИННДовУпр_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_dov_ul2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_fl_info).alias(
            'full_name_lat_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_dov_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_dov_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_dov_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_dov_ul3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__ОКСМ', uluch_fl_info).alias('oksm_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__НаимСтран', uluch_fl_info).alias('name_countr_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__ДатаРег', uluch_fl_info).alias('date_reg_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__РегНомер', uluch_fl_info).alias('reg_num_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__НаимРегОрг', uluch_fl_info).alias('name_reg_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__КодНПСтрРег', uluch_fl_info).alias('np_kode_dov_ul').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн__АдрСтр', uluch_fl_info).alias('address_dov_ul').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_dov_ul4').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_dov_ul4').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_dov_ul4').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_dov_ul4').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ__ОГРНИП', uluch_fl_info).alias('ogrnip_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul_num_dov_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias('egrul_date_dov_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ__Фамилия', uluch_fl_info).alias('surname_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ__Имя', uluch_fl_info).alias('name_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ__Отчество', uluch_fl_info).alias('patronomyc_dov_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ__ИННФЛ', uluch_fl_info).alias('innfl_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_dov_fl2').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_dov_fl2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_dov_fl2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_dov_fl2').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвПолФЛ__Пол', uluch_fl_info).alias('genger_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_dov_fl3').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_dov_fl3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_dov_fl3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_dov_fl3').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ__КодГражд', uluch_fl_info).alias(
            'grazh_type_kode_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ__ОКСМ', uluch_fl_info).alias('grazh_countr_kode_dov_fl').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ__НаимСтран', uluch_fl_info).alias(
            'grazh_countr_name_dov_fl').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_dov_fl5').cast(
            'string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_dov_fl5').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_dov_fl5').cast('string'),
        check_column_exists('_УчрФЛ_СвДовУпрФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_dov_fl5').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл__ОГРНИП', uluch_fl_info).alias('ogrnip_nasl').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл__ДатаОткрНасл', uluch_fl_info).alias('date_nasl').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_ГРНДатаПерв__ГРН', uluch_fl_info).alias('egrul32_num').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_ГРНДатаПерв__ДатаЗаписи', uluch_fl_info).alias('egrul32_date').cast(
            'string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвФЛ__Фамилия', uluch_fl_info).alias('surnam4E').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл__Имя', uluch_fl_info).alias('name4E').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл__Отчество', uluch_fl_info).alias('panronymic4E').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл__ИННФЛ', uluch_fl_info).alias('innfl4E').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл_ГРНДата__ГРН', uluch_fl_info).alias('egrul23_numE').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл_ГРНДата__ДатаЗаписи', uluch_fl_info).alias('egrul23_dateE').cast(
            'string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл_ГРНДатаИспр__ГРН', uluch_fl_info).alias('egrul23_teh_numE').cast(
            'string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СВФл_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul23_teh_dateE').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвПолФЛ__Пол', uluch_fl_info).alias('genger_uch4').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвПолФЛ_ГРНДата__ГРН', uluch_fl_info).alias(
            'egrul_num_genger_uch4').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_genger_uch4').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_genger_uch4').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_genger_uch4').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ__КодГражд', uluch_fl_info).alias('fl_grazh_type_kode3').cast(
            'string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ__ОКСМ', uluch_fl_info).alias('fl_grazh_countr_kode3').cast(
            'string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ__НаимСтран', uluch_fl_info).alias(
            'fl_grazh_countr_name3').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ_ГРНДата__ГРН', uluch_fl_info).alias('egrul_num_grazh3').cast(
            'string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_date_grazh3').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_fl_info).alias(
            'egrul_teh_num_grazh3').cast('string'),
        check_column_exists('_УчрФЛ_ЛицоУпрНасл_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_fl_info).alias(
            'egrul_teh_date_grazh3').cast('string')
    ]
    uluch_fl_info = uluch_fl_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('full_name_dov_ul', f.regexp_replace(f.col('full_name_dov_ul'), '[\n\r\t]', ''))

    return uluch_fl_info

# Might be array might be not if array _УчрРФСубМО if not УчрРФСубМО
def create_uluch_sub_info(df):
    uluch_sub_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                f.col('СвУчредит.*')))

    if 'УчрРФСубМО' in get_arrays(df.select(check_column_exists('СвУчредит.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('_УчрРФСубМО__ОгрДосСв', uluch_sub_info).alias('uch_sub_info_limit').cast('string'),
        check_column_exists('_УчрРФСубМО_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_uch_sub_info').cast('string'),
        check_column_exists('_УчрРФСубМО_ГРНДата__ДатаЗаписи', uluch_sub_info).alias('egrul_date_uch_sub_info').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ГРНДатаИспр__ГРН', uluch_sub_info).alias('egrul_teh_num_uch_sub_info').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_uch_sub_info').cast('string'),
        check_column_exists('_УчрРФСубМО_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul33_num').cast('string'),
        check_column_exists('_УчрРФСубМО_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias('egrul33_date').cast('string'),
        check_column_exists('_УчрРФСубМО_ВидНаимУчр__Код_УчрРФСубМО', uluch_sub_info).alias('mo_code').cast('string'),
        check_column_exists('_УчрРФСубМО_ВидНаимУчр__НаимМО', uluch_sub_info).alias('mo_name').cast('string'),
        check_column_exists('_УчрРФСубМО_ВидНаимУчр__КодРегион', uluch_sub_info).alias('region4_code').cast('string'),
        check_column_exists('_УчрРФСубМО_ВидНаимУчр__НаимРегион', uluch_sub_info).alias('region4_name').cast('string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр__ПризнНедДанУчр', uluch_sub_info).alias('priz_false_uch').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр__ТекстНедДанУчр', uluch_sub_info).alias('text_false_uch').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_sub_info).alias(
            'jud_name_false_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_sub_info).alias(
            'num_decision_false_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_sub_info).alias(
            'date_decision_false_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_false_uch').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_false_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_false_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_false_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап__НоминСтоим', uluch_sub_info).alias('nomprice').cast('string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_ДоляРубля__Числит', uluch_sub_info).alias('numerator_rub').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_ДоляРубля__Знаменат', uluch_sub_info).alias('denominator_rub').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_РазмерДоли_Процент', uluch_sub_info).alias('percent').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_РазмерДоли__ДробДесят', uluch_sub_info).alias('decimals').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_sub_info).alias(
            'nominator').cast('string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_sub_info).alias(
            'denominator').cast('string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_part').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_sub_info).alias('egrul_date_part').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_sub_info).alias('egrul_teh_num_part').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_part').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбъемПрав__ОбъемПрав', uluch_sub_info).alias('scope_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбъемПрав_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_scope_uch').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_scope_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбъемПрав_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_scope_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбъемПрав_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_scope_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul34_num').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
            'egrul34_date').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ__ОГРН', uluch_sub_info).alias('ogrn5').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ__ИНН', uluch_sub_info).alias('inn5').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ__НаимЮЛПолн', uluch_sub_info).alias('ulfull5_name').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДата__ГРН', uluch_sub_info).alias('egrul35_num').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul35_date').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul35_teh_num').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul35_teh_date').cast('string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul36_num').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias('egrul36_date').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СвФЛ__Фамилия', uluch_sub_info).alias('surnam4F').cast('string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл__Имя', uluch_sub_info).alias('name4F').cast('string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл__Отчество', uluch_sub_info).alias('panronymic4F').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл__ИННФЛ', uluch_sub_info).alias('innfl4F').cast('string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДата__ГРН', uluch_sub_info).alias('egrul23_numF').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul23_dateF').cast('string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul23_teh_numF').cast('string'),
        check_column_exists('_УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul23_teh_dateF').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем__ВидОбрем', uluch_sub_info).alias('obrem_typeG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем__СрокОбременения', uluch_sub_info).alias('obrem_termG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_РешСуд__НаимСуда', uluch_sub_info).alias('judge_nameG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_РешСуд__Номер', uluch_sub_info).alias('judge_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_РешСуд__Дата', uluch_sub_info).alias('dudge_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_ГРНДата__ГРН', uluch_sub_info).alias('egrul5_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_ГРНДата__ДатаЗаписи', uluch_sub_info).alias('egrul5_dateG').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_ГРНДатаИспр__ГРН', uluch_sub_info).alias('egrul5_teh_numG').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul5_teh_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_sub_info).alias(
            'egrul6_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
            'egrul6_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_sub_info).alias('surnameG').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_sub_info).alias('nameG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_sub_info).alias(
            'panronymicG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_sub_info).alias('innflG').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul7_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul7_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul7_teh_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul7_teh_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_sub_info).alias('genger_uch').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_genger_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_genger_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_genger_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_genger_uch').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_sub_info).alias(
            'fl_grazh_type_kode').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_sub_info).alias(
            'fl_grazh_countr_kode').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_sub_info).alias(
            'fl_grazh_countr_name').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_grazh').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_grazh').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_grazh').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_grazh').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_sub_info).alias(
            'zal_numberG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_sub_info).alias(
            'zal_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_sub_info).alias(
            'surname2G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_sub_info).alias(
            'name2G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество',
                            uluch_sub_info).alias('patronomyc2G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН',
                            uluch_sub_info).alias('egrul13_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи',
                            uluch_sub_info).alias('egrul13_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН',
                            uluch_sub_info).alias('egrul13_teh_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи',
                            uluch_sub_info).alias('egrul13_teh_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_sub_info).alias(
            'egrul14_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
            'egrul14_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_sub_info).alias('ogrn2G').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_sub_info).alias('inn2G').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_sub_info).alias(
            'ulfull2_nameG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul15_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul15_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul15_teh_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul15_teh_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_sub_info).alias(
            'zalog_in').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_zal_in').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи',
                            uluch_sub_info).alias('egrul_date_zal_in').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_zal_in').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи',
                            uluch_sub_info).alias('egrul_teh_date_zal_in').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_sub_info).alias('oksm2G').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_sub_info).alias(
            'reg2_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_sub_info).alias(
            'reg2_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_sub_info).alias(
            'reg2_nameG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_sub_info).alias(
            'np_kode').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_sub_info).alias(
            'reg2_addressG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul16_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul16_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul16_teh_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul16_teh_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_sub_info).alias(
            'zal2_numberG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_sub_info).alias(
            'zal2_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_sub_info).alias(
            'surname3G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_sub_info).alias(
            'name3G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество',
                            uluch_sub_info).alias('patronomyc3G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_sub_info).alias(
            'innfl3G').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН',
                            uluch_sub_info).alias('egrul17_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи',
                            uluch_sub_info).alias('egrul17_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul17_teh_numG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                            uluch_sub_info).alias('egrul17_teh_dateG').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul_num_uprzal').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_uprzal').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал__Номер', uluch_sub_info).alias(
            'zal_number_uprzal').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал__Дата', uluch_sub_info).alias('zal_date_uprzal').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_sub_info).alias(
            'surname_not_uprzal').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_sub_info).alias(
            'name_not_uprzal').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_sub_info).alias(
            'patronomyc_not_uprzal').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_sub_info).alias(
            'innfl_not_uprzal').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_uprzal1').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_uprzal1').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_uprzal1').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                            uluch_sub_info).alias('egrul_teh_date_uprzal1').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал__Номер', uluch_sub_info).alias(
            'zal_number_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал__Дата', uluch_sub_info).alias('zal_date_uprzal2').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_sub_info).alias(
            'surname_not_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_sub_info).alias(
            'name_not_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_sub_info).alias(
            'patronomyc_not_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_sub_info).alias(
            'innfl_not_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                            uluch_sub_info).alias('egrul_teh_date_uprzal2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_sub_info).alias('ogrnip_zal_fl').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_sub_info).alias(
            'surname_zal_fl').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_sub_info).alias('name_zal_fl').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_sub_info).alias(
            'patronomyc_zal_fl').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_sub_info).alias('innfl_zal_fl').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_zal_fl').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_zal_fl').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_zal_fl').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_zal_fl').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_sub_info).alias(
            'ogrn_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_sub_info).alias('inn_zal_ul').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_sub_info).alias(
            'name_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_sub_info).alias(
            'full_name_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_zal_ul2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи',
                            uluch_sub_info).alias('egrul_date_zal_ul2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_zal_ul2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи',
                            uluch_sub_info).alias('egrul_teh_date_zal_ul2').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_sub_info).alias('oksm_zal_ul').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_sub_info).alias(
            'name_countr_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_sub_info).alias(
            'date_reg_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_sub_info).alias(
            'reg_num_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_sub_info).alias(
            'name_reg_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_sub_info).alias(
            'np_kode_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_sub_info).alias(
            'address_zal_ul').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_sub_info).alias(
            'egrul_num_zal_ul3').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_zal_ul3').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_zal_ul3').cast('string'),
        check_column_exists('_УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_zal_ul3').cast('string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ__ОГРН', uluch_sub_info).alias('ogrn_ulrf').cast('string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ__ИНН', uluch_sub_info).alias('inn_ulrf').cast('string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ__НаимЮЛПолн', uluch_sub_info).alias('full_name_ulrf').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_ulrf').cast(
            'string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_date_ulrf').cast('string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
            'egrul_teh_num_ulrf').cast('string'),
        check_column_exists('_УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
            'egrul_teh_date_ulrf').cast('string')]
    else:
        cols = [
            f.col('table_guid').alias('rodtable_guid'),
            f.col('load_date'),
            f.col('folder_date'),
            check_column_exists('УчрРФСубМО__ОгрДосСв', uluch_sub_info).alias('uch_sub_info_limit').cast('string'),
            check_column_exists('УчрРФСубМО_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_uch_sub_info').cast('string'),
            check_column_exists('УчрРФСубМО_ГРНДата__ДатаЗаписи', uluch_sub_info).alias('egrul_date_uch_sub_info').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ГРНДатаИспр__ГРН', uluch_sub_info).alias('egrul_teh_num_uch_sub_info').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_uch_sub_info').cast('string'),
            check_column_exists('УчрРФСубМО_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul33_num').cast('string'),
            check_column_exists('УчрРФСубМО_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias('egrul33_date').cast('string'),
            check_column_exists('УчрРФСубМО_ВидНаимУчр__КодУчрРФСубМО', uluch_sub_info).alias('mo_code').cast('string'),
            check_column_exists('УчрРФСубМО_ВидНаимУчр__НаимМО', uluch_sub_info).alias('mo_name').cast('string'),
            check_column_exists('УчрРФСубМО_ВидНаимУчр__КодРегион', uluch_sub_info).alias('region4_code').cast('string'),
            check_column_exists('УчрРФСубМО_ВидНаимУчр__НаимРегион', uluch_sub_info).alias('region4_name').cast('string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр__ПризнНедДанУчр', uluch_sub_info).alias('priz_false_uch').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр__ТекстНедДанУчр', uluch_sub_info).alias('text_false_uch').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_sub_info).alias(
                'jud_name_false_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_sub_info).alias(
                'num_decision_false_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_sub_info).alias(
                'date_decision_false_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_false_uch').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_false_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_false_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_false_uch').cast('string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап__НоминСтоим', uluch_sub_info).alias('nomprice').cast('string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_ДоляРубля__Числит', uluch_sub_info).alias('numerator_rub').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_ДоляРубля__Знаменат', uluch_sub_info).alias('denominator_rub').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_РазмерДоли_Процент', uluch_sub_info).alias('percent').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_РазмерДоли__ДробДесят', uluch_sub_info).alias('decimals').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_sub_info).alias(
                'nominator').cast('string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_sub_info).alias(
                'denominator').cast('string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_part').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_sub_info).alias('egrul_date_part').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_sub_info).alias('egrul_teh_num_part').cast(
                'string'),
            check_column_exists('УчрРФСубМО_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_part').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбъемПрав__ОбъемПрав', uluch_sub_info).alias('scope_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбъемПрав_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_scope_uch').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_scope_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбъемПрав_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_scope_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбъемПрав_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_scope_uch').cast('string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul34_num').cast(
                'string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
                'egrul34_date').cast('string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ__ОГРН', uluch_sub_info).alias('ogrn5').cast('string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ__ИНН', uluch_sub_info).alias('inn5').cast('string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ__НаимЮЛПолн', uluch_sub_info).alias('ulfull5_name').cast(
                'string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДата__ГРН', uluch_sub_info).alias('egrul35_num').cast(
                'string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul35_date').cast('string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul35_teh_num').cast('string'),
            check_column_exists('_УчрРФСубМО_СвОргОсущПр_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul35_teh_date').cast('string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul36_num').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias('egrul36_date').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СвФЛ__Фамилия', uluch_sub_info).alias('surnam4F').cast('string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл__Имя', uluch_sub_info).alias('name4F').cast('string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл__Отчество', uluch_sub_info).alias('panronymic4F').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл__ИННФЛ', uluch_sub_info).alias('innfl4F').cast('string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДата__ГРН', uluch_sub_info).alias('egrul23_numF').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul23_dateF').cast('string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul23_teh_numF').cast('string'),
            check_column_exists('УчрРФСубМО_СвФЛОсущПр_СВФл_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul23_teh_dateF').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем__ВидОбрем', uluch_sub_info).alias('obrem_typeG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем__СрокОбременения', uluch_sub_info).alias('obrem_termG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_РешСуд__НаимСуда', uluch_sub_info).alias('judge_nameG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_РешСуд__Номер', uluch_sub_info).alias('judge_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_РешСуд__Дата', uluch_sub_info).alias('dudge_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_ГРНДата__ГРН', uluch_sub_info).alias('egrul5_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_ГРНДата__ДатаЗаписи', uluch_sub_info).alias('egrul5_dateG').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_ГРНДатаИспр__ГРН', uluch_sub_info).alias('egrul5_teh_numG').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul5_teh_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_sub_info).alias(
                'egrul6_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
                'egrul6_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_sub_info).alias('surnameG').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_sub_info).alias('nameG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_sub_info).alias(
                'panronymicG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_sub_info).alias('innflG').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul7_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul7_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul7_teh_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul7_teh_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_sub_info).alias('genger_uch').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_genger_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_genger_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_genger_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_genger_uch').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_sub_info).alias(
                'fl_grazh_type_kode').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_sub_info).alias(
                'fl_grazh_countr_kode').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_sub_info).alias(
                'fl_grazh_countr_name').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_grazh').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_grazh').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_grazh').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_grazh').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_sub_info).alias(
                'zal_numberG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_sub_info).alias(
                'zal_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_sub_info).alias(
                'surname2G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_sub_info).alias(
                'name2G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество',
                                uluch_sub_info).alias('patronomyc2G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН',
                                uluch_sub_info).alias('egrul13_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи',
                                uluch_sub_info).alias('egrul13_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН',
                                uluch_sub_info).alias('egrul13_teh_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи',
                                uluch_sub_info).alias('egrul13_teh_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_sub_info).alias(
                'egrul14_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
                'egrul14_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_sub_info).alias('ogrn2G').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_sub_info).alias('inn2G').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_sub_info).alias(
                'ulfull2_nameG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul15_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul15_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul15_teh_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul15_teh_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_sub_info).alias(
                'zalog_in').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_zal_in').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи',
                                uluch_sub_info).alias('egrul_date_zal_in').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_zal_in').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи',
                                uluch_sub_info).alias('egrul_teh_date_zal_in').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_sub_info).alias('oksm2G').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_sub_info).alias(
                'reg2_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_sub_info).alias(
                'reg2_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_sub_info).alias(
                'reg2_nameG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_sub_info).alias(
                'np_kode').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_sub_info).alias(
                'reg2_addressG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul16_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul16_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul16_teh_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul16_teh_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_sub_info).alias(
                'zal2_numberG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_sub_info).alias(
                'zal2_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_sub_info).alias(
                'surname3G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_sub_info).alias(
                'name3G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество',
                                uluch_sub_info).alias('patronomyc3G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_sub_info).alias(
                'innfl3G').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН',
                                uluch_sub_info).alias('egrul17_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи',
                                uluch_sub_info).alias('egrul17_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul17_teh_numG').cast('string'),
            check_column_exists('УчрРФСубМО_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                                uluch_sub_info).alias('egrul17_teh_dateG').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_ГРНДатаПерв__ГРН', uluch_sub_info).alias('egrul_num_uprzal').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_uprzal').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал__Номер', uluch_sub_info).alias(
                'zal_number_uprzal').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал__Дата', uluch_sub_info).alias('zal_date_uprzal').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_sub_info).alias(
                'surname_not_uprzal').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_sub_info).alias(
                'name_not_uprzal').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_sub_info).alias(
                'patronomyc_not_uprzal').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_sub_info).alias(
                'innfl_not_uprzal').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_uprzal1').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_uprzal1').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_uprzal1').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                                uluch_sub_info).alias('egrul_teh_date_uprzal1').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал__Номер', uluch_sub_info).alias(
                'zal_number_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал__Дата', uluch_sub_info).alias('zal_date_uprzal2').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_sub_info).alias(
                'surname_not_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_sub_info).alias(
                'name_not_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_sub_info).alias(
                'patronomyc_not_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_sub_info).alias(
                'innfl_not_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи',
                                uluch_sub_info).alias('egrul_teh_date_uprzal2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_sub_info).alias('ogrnip_zal_fl').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_sub_info).alias(
                'surname_zal_fl').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_sub_info).alias('name_zal_fl').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_sub_info).alias(
                'patronomyc_zal_fl').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_sub_info).alias('innfl_zal_fl').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_zal_fl').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_zal_fl').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_zal_fl').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_zal_fl').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_sub_info).alias(
                'ogrn_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_sub_info).alias('inn_zal_ul').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_sub_info).alias(
                'name_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_sub_info).alias(
                'full_name_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_zal_ul2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи',
                                uluch_sub_info).alias('egrul_date_zal_ul2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_zal_ul2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи',
                                uluch_sub_info).alias('egrul_teh_date_zal_ul2').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_sub_info).alias('oksm_zal_ul').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_sub_info).alias(
                'name_countr_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_sub_info).alias(
                'date_reg_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_sub_info).alias(
                'reg_num_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_sub_info).alias(
                'name_reg_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_sub_info).alias(
                'np_kode_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_sub_info).alias(
                'address_zal_ul').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_sub_info).alias(
                'egrul_num_zal_ul3').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_zal_ul3').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_zal_ul3').cast('string'),
            check_column_exists('УчрРФСубМО_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_zal_ul3').cast('string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ__ОГРН', uluch_sub_info).alias('ogrn_ulrf').cast('string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ__ИНН', uluch_sub_info).alias('inn_ulrf').cast('string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ__НаимЮЛПолн', uluch_sub_info).alias('full_name_ulrf').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДата__ГРН', uluch_sub_info).alias('egrul_num_ulrf').cast(
                'string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДата__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_date_ulrf').cast('string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДатаИспр__ГРН', uluch_sub_info).alias(
                'egrul_teh_num_ulrf').cast('string'),
            check_column_exists('УчрРФСубМО_СвДовУпрЮЛРФ_ГРНДатаИспр__ДатаЗаписи', uluch_sub_info).alias(
                'egrul_teh_date_ulrf').cast('string')
        ]

    uluch_sub_info = uluch_sub_info.select(*cols).withColumn('table_guid', f.expr("uuid()"))
    return uluch_sub_info

# Может быть массивом и тогда _ может быть и нет тогда без _
def create_uluch_pif_info(df):
    uluch_pif_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                f.col('СвУчредит.*')))

    if 'УчрПИФ' in get_arrays(df.select(check_column_exists('СвУчредит.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                               f.col('load_date'),
                               f.col('folder_date'),
                               check_column_exists('_УчрПИФ_ОгрДосСв__ОгрДосСв', uluch_pif_info).alias('uch_pif_info_limit').cast('string'),
                               check_column_exists('_УчрПИФ_ОгрДосСв_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_uch_pif_info').cast('string'),
                               check_column_exists('_УчрПИФ_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uch_pif_info').cast('string'),
                               check_column_exists('_УчрПИФ_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_uch_pif_info').cast('string'),
                               check_column_exists('_УчрПИФ_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_uch_pif_info').cast('string'),
                               check_column_exists('_УчрПИФ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul38_num').cast('string'),
                               check_column_exists('_УчрПИФ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul38_date').cast('string'),
                               check_column_exists('_УчрПИФ_СвНаимПИФ__НаимПИФ', uluch_pif_info).alias('pif_name').cast('string'),
                               check_column_exists('_УчрПИФ_СвНаимПИФ_ГРНДата__ГРН', uluch_pif_info).alias('egrul39_num').cast('string'),
                               check_column_exists('_УчрПИФ_СвНаимПИФ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul39_date').cast('string'),
                               check_column_exists('_УчрПИФ_СвНаимПИФ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul38_teh_num').cast('string'),
                               check_column_exists('_УчрПИФ_СвНаимПИФ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul38_teh_date').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр__ПризнНедДанУчр', uluch_pif_info).alias('priz_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр__ТекстНедДанУчр', uluch_pif_info).alias('text_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_pif_info).alias('jud_name_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_pif_info).alias('num_decision_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_pif_info).alias('date_decision_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_false_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul40_num').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul40_date').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ__ОГРН', uluch_pif_info).alias('ogrn6').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ__ИНН', uluch_pif_info).alias('inn6').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ__НаимЮЛПолн', uluch_pif_info).alias('ulfull6_name').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ_ГРНДата__ГРН', uluch_pif_info).alias('egrul41_num').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul41_date').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul41_teh_num').cast('string'),
                               check_column_exists('_УчрПИФ_СвУпрКомпПИФ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul41_teh_date').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап__НоминСтоим', uluch_pif_info).alias('nomprice').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_ДоляРубля__Числит', uluch_pif_info).alias('numerator_rub').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_ДоляРубля__Знаменат', uluch_pif_info).alias('denominator_rub').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_РазмерДоли_Процент', uluch_pif_info).alias('percent').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_РазмерДоли_ДробДесят', uluch_pif_info).alias('decimals').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_pif_info).alias('nominator').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_pif_info).alias('denominator').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_part').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_part').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_part').cast('string'),
                               check_column_exists('_УчрПИФ_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_part').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбъемПрав__ОбъемПрав', uluch_pif_info).alias('scope_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбъемПрав_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_scope_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_scope_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_scope_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_scope_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем__ВидОбрем', uluch_pif_info).alias('obrem_typeH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем__СрокОбременения', uluch_pif_info).alias('obrem_termH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_РешСуд__НаимСуда', uluch_pif_info).alias('judge_nameH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_РешСуд__Номер', uluch_pif_info).alias('judge_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_РешСуд__Дата', uluch_pif_info).alias('dudge_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_ГРНДата__ГРН', uluch_pif_info).alias('egrul5_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul5_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul5_teh_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul5_teh_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul6_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul6_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_pif_info).alias('surnameH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_pif_info).alias('nameH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_pif_info).alias('panronymicH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_pif_info).alias('innflH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul7_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul7_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul7_teh_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul7_teh_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_pif_info).alias('genger_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_genger_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_genger_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_genger_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_genger_uch').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_pif_info).alias('fl_grazh_type_kode').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_pif_info).alias('fl_grazh_countr_kode').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_pif_info).alias('fl_grazh_countr_name').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_grazh').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_grazh').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_grazh').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_grazh').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_pif_info).alias('zal_numberH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_pif_info).alias('zal_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_pif_info).alias('surname2H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_pif_info).alias('name2H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_pif_info).alias('patronomyc2H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul13_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul13_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul13_teh_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul13_teh_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul14_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul14_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_pif_info).alias('ogrn2H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_pif_info).alias('inn2H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_pif_info).alias('ulfull2_nameH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul15_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul15_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul15_teh_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul15_teh_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_pif_info).alias('zalog_in').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_in').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_in').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_in').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_in').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_pif_info).alias('oksm2H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_pif_info).alias('reg2_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_pif_info).alias('reg2_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_pif_info).alias('reg2_nameH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_pif_info).alias('np_kode').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_pif_info).alias('reg2_addressH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul16_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul16_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul16_teh_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul16_teh_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_pif_info).alias('zal2_numberH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_pif_info).alias('zal2_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_pif_info).alias('surname3H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_pif_info).alias('name3H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_pif_info).alias('patronomyc3H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_pif_info).alias('innfl3H').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul17_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul17_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul17_teh_numH').cast('string'),
                               check_column_exists('_УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul17_teh_dateH').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul_num_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал__Номер', uluch_pif_info).alias('zal_number_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал__Дата', uluch_pif_info).alias('zal_date_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_pif_info).alias('surname_not_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_pif_info).alias('name_not_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_pif_info).alias('patronomyc_not_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_pif_info).alias('innfl_not_uprzal').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_uprzal1').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uprzal1').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_uprzal1').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_uprzal1').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал__Номер', uluch_pif_info).alias('zal_number_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал__Дата', uluch_pif_info).alias('zal_date_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_pif_info).alias('surname_not_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_pif_info).alias('name_not_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_pif_info).alias('patronomyc_not_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_pif_info).alias('innfl_not_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_uprzal2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_pif_info).alias('ogrnip_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_pif_info).alias('surname_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_pif_info).alias('name_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_pif_info).alias('patronomyc_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_pif_info).alias('innfl_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_fl').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_pif_info).alias('ogrn_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_pif_info).alias('inn_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_pif_info).alias('name_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_pif_info).alias('full_name_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_ul2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_ul2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_pif_info).alias('oksm_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_pif_info).alias('name_countr_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_pif_info).alias('date_reg_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_pif_info).alias('reg_num_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_pif_info).alias('name_reg_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_pif_info).alias('np_kode_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_pif_info).alias('address_zal_ul').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_ul3').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_ul3').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                               check_column_exists('_УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_ul3').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                               f.col('load_date'),
                               f.col('folder_date'),
                               check_column_exists('УчрПИФ_ОгрДосСв__ОгрДосСв', uluch_pif_info).alias('uch_pif_info_limit').cast('string'),
                               check_column_exists('УчрПИФ_ОгрДосСв_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_uch_pif_info').cast('string'),
                               check_column_exists('УчрПИФ_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uch_pif_info').cast('string'),
                               check_column_exists('УчрПИФ_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_uch_pif_info').cast('string'),
                               check_column_exists('УчрПИФ_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_uch_pif_info').cast('string'),
                               check_column_exists('УчрПИФ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul38_num').cast('string'),
                               check_column_exists('УчрПИФ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul38_date').cast('string'),
                               check_column_exists('УчрПИФ_СвНаимПИФ__НаимПИФ', uluch_pif_info).alias('pif_name').cast('string'),
                               check_column_exists('УчрПИФ_СвНаимПИФ_ГРНДата__ГРН', uluch_pif_info).alias('egrul39_num').cast('string'),
                               check_column_exists('УчрПИФ_СвНаимПИФ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul39_date').cast('string'),
                               check_column_exists('УчрПИФ_СвНаимПИФ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul38_teh_num').cast('string'),
                               check_column_exists('УчрПИФ_СвНаимПИФ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul38_teh_date').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр__ПризнНедДанУчр', uluch_pif_info).alias('priz_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр__ТекстНедДанУчр', uluch_pif_info).alias('text_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_pif_info).alias('jud_name_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_pif_info).alias('num_decision_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_pif_info).alias('date_decision_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_false_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul40_num').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul40_date').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ__ОГРН', uluch_pif_info).alias('ogrn6').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ__ИНН', uluch_pif_info).alias('inn6').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ__НаимЮЛПолн', uluch_pif_info).alias('ulfull6_name').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ_ГРНДата__ГРН', uluch_pif_info).alias('egrul41_num').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul41_date').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul41_teh_num').cast('string'),
                               check_column_exists('УчрПИФ_СвУпрКомпПИФ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul41_teh_date').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап__НоминСтоим', uluch_pif_info).alias('nomprice').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_ДоляРубля__Числит', uluch_pif_info).alias('numerator_rub').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_ДоляРубля__Знаменат', uluch_pif_info).alias('denominator_rub').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_РазмерДоли_Процент', uluch_pif_info).alias('percent').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_РазмерДоли_ДробДесят', uluch_pif_info).alias('decimals').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_pif_info).alias('nominator').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_pif_info).alias('denominator').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_part').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_part').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_part').cast('string'),
                               check_column_exists('УчрПИФ_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_part').cast('string'),
                               check_column_exists('УчрПИФ_СвОбъемПрав__ОбъемПрав', uluch_pif_info).alias('scope_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбъемПрав_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_scope_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_scope_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_scope_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_scope_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем__ВидОбрем', uluch_pif_info).alias('obrem_typeH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем__СрокОбременения', uluch_pif_info).alias('obrem_termH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_РешСуд__НаимСуда', uluch_pif_info).alias('judge_nameH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_РешСуд__Номер', uluch_pif_info).alias('judge_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_РешСуд__Дата', uluch_pif_info).alias('dudge_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_ГРНДата__ГРН', uluch_pif_info).alias('egrul5_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul5_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul5_teh_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul5_teh_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul6_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul6_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_pif_info).alias('surnameH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_pif_info).alias('nameH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_pif_info).alias('panronymicH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_pif_info).alias('innflH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul7_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul7_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul7_teh_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul7_teh_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_pif_info).alias('genger_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_genger_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_genger_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_genger_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_genger_uch').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_pif_info).alias('fl_grazh_type_kode').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_pif_info).alias('fl_grazh_countr_kode').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_pif_info).alias('fl_grazh_countr_name').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_grazh').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_grazh').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_grazh').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_grazh').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_pif_info).alias('zal_numberH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_pif_info).alias('zal_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_pif_info).alias('surname2H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_pif_info).alias('name2H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_pif_info).alias('patronomyc2H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul13_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul13_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul13_teh_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul13_teh_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul14_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul14_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_pif_info).alias('ogrn2H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_pif_info).alias('inn2H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_pif_info).alias('ulfull2_nameH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul15_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul15_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul15_teh_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul15_teh_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_pif_info).alias('zalog_in').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_in').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_in').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_in').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_in').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_pif_info).alias('oksm2H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_pif_info).alias('reg2_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_pif_info).alias('reg2_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_pif_info).alias('reg2_nameH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_pif_info).alias('np_kode').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_pif_info).alias('reg2_addressH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul16_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul16_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul16_teh_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul16_teh_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_pif_info).alias('zal2_numberH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_pif_info).alias('zal2_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_pif_info).alias('surname3H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_pif_info).alias('name3H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_pif_info).alias('patronomyc3H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_pif_info).alias('innfl3H').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul17_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul17_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul17_teh_numH').cast('string'),
                               check_column_exists('УчрПИФ_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul17_teh_dateH').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_ГРНДатаПерв__ГРН', uluch_pif_info).alias('egrul_num_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал__Номер', uluch_pif_info).alias('zal_number_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал__Дата', uluch_pif_info).alias('zal_date_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_pif_info).alias('surname_not_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_pif_info).alias('name_not_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_pif_info).alias('patronomyc_not_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_pif_info).alias('innfl_not_uprzal').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_uprzal1').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uprzal1').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_uprzal1').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_uprzal1').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал__Номер', uluch_pif_info).alias('zal_number_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал__Дата', uluch_pif_info).alias('zal_date_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_pif_info).alias('surname_not_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_pif_info).alias('name_not_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_pif_info).alias('patronomyc_not_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_pif_info).alias('innfl_not_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_uprzal2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_pif_info).alias('ogrnip_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_pif_info).alias('surname_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_pif_info).alias('name_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_pif_info).alias('patronomyc_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_pif_info).alias('innfl_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_fl').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_pif_info).alias('ogrn_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_pif_info).alias('inn_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_pif_info).alias('name_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_pif_info).alias('full_name_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_ul2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_ul2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_pif_info).alias('oksm_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_pif_info).alias('name_countr_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_pif_info).alias('date_reg_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_pif_info).alias('reg_num_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_pif_info).alias('name_reg_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_pif_info).alias('np_kode_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_pif_info).alias('address_zal_ul').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_pif_info).alias('egrul_num_zal_ul3').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_pif_info).alias('egrul_date_zal_ul3').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_pif_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                               check_column_exists('УчрПИФ_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_pif_info).alias('egrul_teh_date_zal_ul3').cast('string')]

    uluch_pif_info = uluch_pif_info.select(*cols).withColumn('table_guid', f.expr("uuid()"))
    return uluch_pif_info

# Не найдено тега УчрДогИнвТов в последних загрузках, возможно эти сведения не отдают нам
def create_uluch_doginv_info(df):
    uluch_doginv_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                f.col('СвУчредит.*')))

    uluch_doginv_info = uluch_doginv_info.select(f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('УчрДогИнвТов_ОгрДосСв__ОгрДосСв', uluch_doginv_info).alias('uch_doginv_info_limit').cast('string'),
                                check_column_exists('УчрДогИнвТов_ОгрДосСв_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uch_doginv_info').cast('string'),
                                check_column_exists('УчрДогИнвТов_ОгрДосСв_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uch_doginv_info').cast('string'),
                                check_column_exists('УчрДогИнвТов_ОгрДосСв_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uch_doginv_info').cast('string'),
                                check_column_exists('УчрДогИнвТов_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uch_doginv_info').cast('string'),
                                check_column_exists('УчрДогИнвТов_ГРНДатаПерв__ГРН', uluch_doginv_info).alias('egrul_num_uch_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ГРНДатаПерв__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uch_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов__НаимДог', uluch_doginv_info).alias('name_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов__НомерДог', uluch_doginv_info).alias('reg_num_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов__Дата', uluch_doginv_info).alias('date_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ФИОНотариус__Фамилия', uluch_doginv_info).alias('surname_not_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ФИОНотариус__Имя', uluch_doginv_info).alias('name_not_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ФИОНотариус__Отчество', uluch_doginv_info).alias('patronomyc_not_doginv').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uch_doginv2').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uch_doginv2').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uch_doginv2').cast('string'),
                                check_column_exists('УчрДогИнвТов_ИнПрДогИнвТов_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uch_doginv2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ__ОГРН', uluch_doginv_info).alias('ogrn_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ__ИНН', uluch_doginv_info).alias('inn_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ__НаимЮЛПолн', uluch_doginv_info).alias('full_name_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ__ОГРН', uluch_doginv_info).alias('ogrn_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ__ИНН', uluch_doginv_info).alias('inn_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_doginv_info).alias('ulfull_name_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__ОКСМ', uluch_doginv_info).alias('oksm_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__НаимСтран', uluch_doginv_info).alias('name_countr_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__ДатаРег', uluch_doginv_info).alias('date_reg_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__РегНомер', uluch_doginv_info).alias('reg_num_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__НаимРегОрг', uluch_doginv_info).alias('name_reg_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__КодНПСтрРег', uluch_doginv_info).alias('np_kode_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн__АдрСтр', uluch_doginv_info).alias('address_uptov_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uptov_in2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uptov_in2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uptov_in2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uptov_in2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвПредЮЛ__НаимПредЮЛ', uluch_doginv_info).alias('name_predul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвПредЮЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_predul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвПредЮЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_predul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвПредЮЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_predul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвПредЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_predul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвАкРАФП__НомерРАФП', uluch_doginv_info).alias('rapf_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвАкРАФП_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_rapf_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвАкРАФП_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_rapf_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвАкРАФП_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_rapf_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУпТовИнЮЛ_СвАкРАФП_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_rapf_uptov').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр__ПризнНедДанУчр', uluch_doginv_info).alias('priz_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр__ТекстНедДанУчр', uluch_doginv_info).alias('text_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_РешСудНедДанУчр__НаимСуда', uluch_doginv_info).alias('jud_name_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_РешСудНедДанУчр__Номер', uluch_doginv_info).alias('num_decision_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_РешСудНедДанУчр__Дата', uluch_doginv_info).alias('date_decision_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвНедДанУчр_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_false_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап__НоминСтоим', uluch_doginv_info).alias('nomprice').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_ДоляРубля__Числит', uluch_doginv_info).alias('numerator_rub').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_ДоляРубля__Знаменат', uluch_doginv_info).alias('denominator_rub').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_РазмерДоли__Процент', uluch_doginv_info).alias('percent').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_РазмерДоли__ДробДесят', uluch_doginv_info).alias('decimals').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_РазмерДоли_ДробПрост__Числит', uluch_doginv_info).alias('nominator').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_РазмерДоли_ДробПрост__Знаменат', uluch_doginv_info).alias('denominator').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_part').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_part').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_part').cast('string'),
                                check_column_exists('УчрДогИнвТов_ДоляУстКап_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_part').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбъемПрав__ОбъемПрав', uluch_doginv_info).alias('scope_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбъемПрав_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_scope_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбъемПрав_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_scope_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбъемПрав_ГРНДатаГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_scope_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбъемПрав_ГРНДатаГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_scope_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем__ВидОбрем', uluch_doginv_info).alias('obrem_typeH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем__СрокОбременения', uluch_doginv_info).alias('obrem_termH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_РешСуд__НаимСуда', uluch_doginv_info).alias('judge_nameH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_РешСуд__Номер', uluch_doginv_info).alias('judge_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_РешСуд__Дата', uluch_doginv_info).alias('dudge_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_ГРНДата__ГРН', uluch_doginv_info).alias('egrul5_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul5_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul5_teh_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul5_teh_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', uluch_doginv_info).alias('egrul6_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', uluch_doginv_info).alias('egrul6_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', uluch_doginv_info).alias('surnameH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', uluch_doginv_info).alias('nameH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', uluch_doginv_info).alias('panronymicH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', uluch_doginv_info).alias('innflH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul7_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul7_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul7_teh_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul7_teh_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', uluch_doginv_info).alias('genger_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_genger_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_genger_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_genger_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_genger_uch').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', uluch_doginv_info).alias('fl_grazh_type_kode').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', uluch_doginv_info).alias('fl_grazh_countr_kode').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', uluch_doginv_info).alias('fl_grazh_countr_name').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_grazh').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_grazh').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_grazh').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_grazh').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', uluch_doginv_info).alias('zal_numberH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', uluch_doginv_info).alias('zal_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', uluch_doginv_info).alias('surname2H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', uluch_doginv_info).alias('name2H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', uluch_doginv_info).alias('patronomyc2H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', uluch_doginv_info).alias('egrul13_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul13_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul13_teh_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul13_teh_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', uluch_doginv_info).alias('egrul14_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', uluch_doginv_info).alias('egrul14_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', uluch_doginv_info).alias('ogrn2H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', uluch_doginv_info).alias('inn2H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_doginv_info).alias('ulfull2_nameH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul15_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul15_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul15_teh_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul15_teh_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_doginv_info).alias('zalog_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_zal_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_zal_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_zal_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_zal_in').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', uluch_doginv_info).alias('oksm2H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', uluch_doginv_info).alias('reg2_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', uluch_doginv_info).alias('reg2_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', uluch_doginv_info).alias('reg2_nameH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', uluch_doginv_info).alias('np2_kode').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', uluch_doginv_info).alias('reg2_addressH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', uluch_doginv_info).alias('egrul16_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul16_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul16_teh_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul16_teh_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', uluch_doginv_info).alias('zal2_numberH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', uluch_doginv_info).alias('zal2_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_doginv_info).alias('surname3H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', uluch_doginv_info).alias('name3H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', uluch_doginv_info).alias('patronomyc3H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_doginv_info).alias('innfl3H').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_doginv_info).alias('egrul17_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul17_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul17_teh_numH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul17_teh_dateH').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_ГРНДатаПерв__ГРН', uluch_doginv_info).alias('egrul_num_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_ГРНДатаПерв__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал__Номер', uluch_doginv_info).alias('zal_number_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал__Дата', uluch_doginv_info).alias('zal_date_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', uluch_doginv_info).alias('surname_not_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', uluch_doginv_info).alias('name_not_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', uluch_doginv_info).alias('patronomyc_not_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', uluch_doginv_info).alias('innfl_not_uprzal').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uprzal1').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uprzal1').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uprzal1').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uprzal1').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал__Номер', uluch_doginv_info).alias('zal_number_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал__Дата', uluch_doginv_info).alias('zal_date_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', uluch_doginv_info).alias('surname_not_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус__Имя', uluch_doginv_info).alias('name_not_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', uluch_doginv_info).alias('patronomyc_not_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', uluch_doginv_info).alias('innfl_not_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_uprzal2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ__ОГРНИП', uluch_doginv_info).alias('ogrnip_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', uluch_doginv_info).alias('surname_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', uluch_doginv_info).alias('name_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', uluch_doginv_info).alias('patronomyc_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', uluch_doginv_info).alias('innfl_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_zal_fl').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', uluch_doginv_info).alias('ogrn_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', uluch_doginv_info).alias('inn_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', uluch_doginv_info).alias('name_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', uluch_doginv_info).alias('full_name_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_zal_ul2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_zal_ul2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', uluch_doginv_info).alias('oksm_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', uluch_doginv_info).alias('name_countr_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', uluch_doginv_info).alias('date_reg_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', uluch_doginv_info).alias('reg_num_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', uluch_doginv_info).alias('name_reg_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', uluch_doginv_info).alias('np_kode_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', uluch_doginv_info).alias('address_zal_ul').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', uluch_doginv_info).alias('egrul_num_zal_ul3').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', uluch_doginv_info).alias('egrul_date_zal_ul3').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', uluch_doginv_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                                check_column_exists('УчрДогИнвТов_СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', uluch_doginv_info).alias('egrul_teh_date_zal_ul3').cast('string')
                                ).withColumn('table_guid', f.expr("uuid()"))
    return uluch_doginv_info


def create_ulustcapdol_info(df):
    ulustcapdol_info = spark_read(df.select(
                                f.col('table_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('СвДоляООО.*', df)))

    if 'СвОбрем' in get_arrays(df.select(check_column_exists('СвДоляООО.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('_НоминСтоим', ulustcapdol_info).alias('nomprice').cast('string'),
                                check_column_exists('ДоляРубля__Числит', ulustcapdol_info).alias('numerator_rub').cast('string'),
                                check_column_exists('ДоляРубля__Знаменат', ulustcapdol_info).alias('denominator_rub').cast('string'),
                                check_column_exists('РазмерДоли_Процент', ulustcapdol_info).alias('percent').cast('string'),
                                check_column_exists('РазмерДоли_ДробДесят', ulustcapdol_info).alias('decimals').cast('string'),
                                check_column_exists('РазмерДоли_ДробПрост__Числит', ulustcapdol_info).alias('nominator').cast('string'),
                                check_column_exists('РазмерДоли_ДробПрост__Знаменат', ulustcapdol_info).alias('denominator').cast('string'),
                                check_column_exists('ГРНДата__ГРН', ulustcapdol_info).alias('egrul4_num').cast('string'),
                                check_column_exists('ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul4_date').cast('string'),
                                check_column_exists('ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul4_teh_num').cast('string'),
                                check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul4_teh_date').cast('string'),
                                check_column_exists('_СвОбрем__ВидОбрем', ulustcapdol_info).alias('obrem_typeH').cast('string'),
                                check_column_exists('_СвОбрем__СрокОбременения', ulustcapdol_info).alias('obrem_termH').cast('string'),
                                check_column_exists('_СвОбрем_РешСуд__НаимСуда', ulustcapdol_info).alias('judge_nameH').cast('string'),
                                check_column_exists('_СвОбрем_РешСуд__Номер', ulustcapdol_info).alias('judge_numH').cast('string'),
                                check_column_exists('_СвОбрем_РешСуд__Дата', ulustcapdol_info).alias('dudge_dateH').cast('string'),
                                check_column_exists('_СвОбрем_ГРНДата__ГРН', ulustcapdol_info).alias('egrul5_numH').cast('string'),
                                check_column_exists('_СвОбрем_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul5_dateH').cast('string'),
                                check_column_exists('_СвОбрем_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul5_teh_numH').cast('string'),
                                check_column_exists('_СвОбрем_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul5_teh_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', ulustcapdol_info).alias('egrul6_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', ulustcapdol_info).alias('egrul6_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', ulustcapdol_info).alias('surnameH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', ulustcapdol_info).alias('nameH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', ulustcapdol_info).alias('panronymicH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', ulustcapdol_info).alias('innflH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul7_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul7_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul7_teh_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul7_teh_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', ulustcapdol_info).alias('genger').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_genger').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_genger').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_genger').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_genger').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', ulustcapdol_info).alias('fl_grazh_type_kode').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', ulustcapdol_info).alias('fl_grazh_countr_kode').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', ulustcapdol_info).alias('fl_grazh_countr_name').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_grazh').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_grazh').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_grazh').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_grazh').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', ulustcapdol_info).alias('zal_numberH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', ulustcapdol_info).alias('zal_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', ulustcapdol_info).alias('surname2H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', ulustcapdol_info).alias('name2H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', ulustcapdol_info).alias('patronomyc2H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul13_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul13_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul13_teh_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul13_teh_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', ulustcapdol_info).alias('egrul14_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', ulustcapdol_info).alias('egrul14_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', ulustcapdol_info).alias('ogrn2H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', ulustcapdol_info).alias('inn2H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', ulustcapdol_info).alias('ulfull2_nameH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul15_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul15_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul15_teh_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul15_teh_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', ulustcapdol_info).alias('zalog_in').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_in').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_in').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_in').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_in').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', ulustcapdol_info).alias('oksm2H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', ulustcapdol_info).alias('reg2_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', ulustcapdol_info).alias('reg2_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', ulustcapdol_info).alias('reg2_nameH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', ulustcapdol_info).alias('np2_kode').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', ulustcapdol_info).alias('reg2_addressH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul16_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul16_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul16_teh_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul16_teh_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', ulustcapdol_info).alias('zal2_numberH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', ulustcapdol_info).alias('zal2_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', ulustcapdol_info).alias('surname3H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', ulustcapdol_info).alias('name3H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', ulustcapdol_info).alias('patronomyc3H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', ulustcapdol_info).alias('innfl3H').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul17_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul17_dateH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul17_teh_numH').cast('string'),
                                check_column_exists('_СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul17_teh_dateH').cast('string'),
                                check_column_exists('СвУправЗал_ГРНДатаПерв__ГРН', ulustcapdol_info).alias('egrul_num_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_ГРНДатаПерв__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал__Номер', ulustcapdol_info).alias('zal_number_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал__Дата', ulustcapdol_info).alias('zal_date_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', ulustcapdol_info).alias('surname_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', ulustcapdol_info).alias('name_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', ulustcapdol_info).alias('patronomyc_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', ulustcapdol_info).alias('innfl_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал__Номер', ulustcapdol_info).alias('zal_number_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал__Дата', ulustcapdol_info).alias('zal_date_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', ulustcapdol_info).alias('surname_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__Имя', ulustcapdol_info).alias('name_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', ulustcapdol_info).alias('patronomyc_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', ulustcapdol_info).alias('innfl_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ__ОГРНИП', ulustcapdol_info).alias('ogrnip_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', ulustcapdol_info).alias('surname_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', ulustcapdol_info).alias('name_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', ulustcapdol_info).alias('patronomyc_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', ulustcapdol_info).alias('innfl_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', ulustcapdol_info).alias('ogrn_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', ulustcapdol_info).alias('inn_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', ulustcapdol_info).alias('name_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', ulustcapdol_info).alias('full_name_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', ulustcapdol_info).alias('oksm_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', ulustcapdol_info).alias('name_countr_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', ulustcapdol_info).alias('date_reg_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', ulustcapdol_info).alias('reg_num_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', ulustcapdol_info).alias('name_reg_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', ulustcapdol_info).alias('np_kode_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', ulustcapdol_info).alias('address_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_ul3').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_ul3').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_ul3').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                f.col('load_date'),
                                f.col('folder_date'),
                                check_column_exists('_НоминСтоим', ulustcapdol_info).alias('nomprice').cast('string'),
                                check_column_exists('ДоляРубля__Числит', ulustcapdol_info).alias('numerator_rub').cast('string'),
                                check_column_exists('ДоляРубля__Знаменат', ulustcapdol_info).alias('denominator_rub').cast('string'),
                                check_column_exists('РазмерДоли_Процент', ulustcapdol_info).alias('percent').cast('string'),
                                check_column_exists('РазмерДоли_ДробДесят', ulustcapdol_info).alias('decimals').cast('string'),
                                check_column_exists('РазмерДоли_ДробПрост__Числит', ulustcapdol_info).alias('nominator').cast('string'),
                                check_column_exists('РазмерДоли_ДробПрост__Знаменат', ulustcapdol_info).alias('denominator').cast('string'),
                                check_column_exists('ГРНДата__ГРН', ulustcapdol_info).alias('egrul4_num').cast('string'),
                                check_column_exists('ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul4_date').cast('string'),
                                check_column_exists('ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul4_teh_num').cast('string'),
                                check_column_exists('ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul4_teh_date').cast('string'),
                                check_column_exists('СвОбрем__ВидОбрем', ulustcapdol_info).alias('obrem_typeH').cast('string'),
                                check_column_exists('СвОбрем__СрокОбременения', ulustcapdol_info).alias('obrem_termH').cast('string'),
                                check_column_exists('СвОбрем_РешСуд__НаимСуда', ulustcapdol_info).alias('judge_nameH').cast('string'),
                                check_column_exists('СвОбрем_РешСуд__Номер', ulustcapdol_info).alias('judge_numH').cast('string'),
                                check_column_exists('СвОбрем_РешСуд__Дата', ulustcapdol_info).alias('dudge_dateH').cast('string'),
                                check_column_exists('СвОбрем_ГРНДата__ГРН', ulustcapdol_info).alias('egrul5_numH').cast('string'),
                                check_column_exists('СвОбрем_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul5_dateH').cast('string'),
                                check_column_exists('СвОбрем_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul5_teh_numH').cast('string'),
                                check_column_exists('СвОбрем_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul5_teh_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ГРН', ulustcapdol_info).alias('egrul6_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_ГРНДатаПерв__ДатаЗаписи', ulustcapdol_info).alias('egrul6_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ__Фамилия', ulustcapdol_info).alias('surnameH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ__Имя', ulustcapdol_info).alias('nameH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ__Отчество', ulustcapdol_info).alias('panronymicH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ__ИННФЛ', ulustcapdol_info).alias('innflH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul7_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul7_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul7_teh_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul7_teh_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвПолФЛ__Пол', ulustcapdol_info).alias('genger').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_genger').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_genger').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_genger').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвПолФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_genger').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__КодГражд', ulustcapdol_info).alias('fl_grazh_type_kode').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__ОКСМ', ulustcapdol_info).alias('fl_grazh_countr_kode').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ__НаимСтран', ulustcapdol_info).alias('fl_grazh_countr_name').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_grazh').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_grazh').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_grazh').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвГраждФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_grazh').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Номер', ulustcapdol_info).alias('zal_numberH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал__Дата', ulustcapdol_info).alias('zal_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Фамилия', ulustcapdol_info).alias('surname2H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Имя', ulustcapdol_info).alias('name2H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус__Отчество', ulustcapdol_info).alias('patronomyc2H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul13_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul13_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul13_teh_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержФЛ_СвНотУдДогЗал_CвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul13_teh_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ГРН', ulustcapdol_info).alias('egrul14_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_ГРНДатаПерв__ДатаЗаписи', ulustcapdol_info).alias('egrul14_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ОГРН', ulustcapdol_info).alias('ogrn2H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__ИНН', ulustcapdol_info).alias('inn2H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ__НаимЮЛПолн', ulustcapdol_info).alias('ulfull2_nameH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul15_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul15_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul15_teh_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul15_teh_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн__НаимПолн', ulustcapdol_info).alias('zalog_in').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_in').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_in').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_in').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_in').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн__ОКСМ', ulustcapdol_info).alias('oksm2H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн__ДатаРег', ulustcapdol_info).alias('reg2_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн__РегНомер', ulustcapdol_info).alias('reg2_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн__НаимРегОрг', ulustcapdol_info).alias('reg2_nameH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн__КодНПСтрРег', ulustcapdol_info).alias('np2_kode').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн__АдрСтр', ulustcapdol_info).alias('reg2_addressH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul16_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul16_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul16_teh_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul16_teh_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Номер', ulustcapdol_info).alias('zal2_numberH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал__Дата', ulustcapdol_info).alias('zal2_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Фамилия', ulustcapdol_info).alias('surname3H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Имя', ulustcapdol_info).alias('name3H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__Отчество', ulustcapdol_info).alias('patronomyc3H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус__ИННФЛ', ulustcapdol_info).alias('innfl3H').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul17_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul17_dateH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul17_teh_numH').cast('string'),
                                check_column_exists('СвОбрем_СвЗалогДержЮЛ_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul17_teh_dateH').cast('string'),
                                check_column_exists('СвУправЗал_ГРНДатаПерв__ГРН', ulustcapdol_info).alias('egrul_num_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_ГРНДатаПерв__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал__Номер', ulustcapdol_info).alias('zal_number_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал__Дата', ulustcapdol_info).alias('zal_date_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__Фамилия', ulustcapdol_info).alias('surname_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__Имя', ulustcapdol_info).alias('name_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__Отчество', ulustcapdol_info).alias('patronomyc_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус__ИННФЛ', ulustcapdol_info).alias('innfl_not_uprzal').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвНотУдДогЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_uprzal1').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал__Номер', ulustcapdol_info).alias('zal_number_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал__Дата', ulustcapdol_info).alias('zal_date_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__Фамилия', ulustcapdol_info).alias('surname_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__Имя', ulustcapdol_info).alias('name_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__Отчество', ulustcapdol_info).alias('patronomyc_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус__ИННФЛ', ulustcapdol_info).alias('innfl_not_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвДогУправЗал_СвНотариус_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_uprzal2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ__ОГРНИП', ulustcapdol_info).alias('ogrnip_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__Фамилия', ulustcapdol_info).alias('surname_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__Имя', ulustcapdol_info).alias('name_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__Отчество', ulustcapdol_info).alias('patronomyc_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ__ИННФЛ', ulustcapdol_info).alias('innfl_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалФЛ_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_fl').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ОГРН', ulustcapdol_info).alias('ogrn_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__ИНН', ulustcapdol_info).alias('inn_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ__НаимЮЛПолн', ulustcapdol_info).alias('name_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_НаимИННЮЛ_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн__НаимПолн', ulustcapdol_info).alias('full_name_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвНаимЮЛПолнИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_ul2').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ОКСМ', ulustcapdol_info).alias('oksm_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимСтран', ulustcapdol_info).alias('name_countr_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__ДатаРег', ulustcapdol_info).alias('date_reg_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__РегНомер', ulustcapdol_info).alias('reg_num_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__НаимРегОрг', ulustcapdol_info).alias('name_reg_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__КодНПСтрРег', ulustcapdol_info).alias('np_kode_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн__АдрСтр', ulustcapdol_info).alias('address_zal_ul').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ГРН', ulustcapdol_info).alias('egrul_num_zal_ul3').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДата__ДатаЗаписи', ulustcapdol_info).alias('egrul_date_zal_ul3').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ГРН', ulustcapdol_info).alias('egrul_teh_num_zal_ul3').cast('string'),
                                check_column_exists('СвУправЗал_СвУпрЗалЮЛ_СвРегИн_ГРНДатаИспр__ДатаЗаписи', ulustcapdol_info).alias('egrul_teh_date_zal_ul3').cast('string')]

    ulustcapdol_info = ulustcapdol_info.select(*cols).withColumn('table_guid', f.expr("uuid()"))
    return ulustcapdol_info


def create_ulacsderg_info(df):
    ulacsderg_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвДержРеестрАО.*', df)))

    ulacsderg_info = ulacsderg_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('ГРНДатаПерв__ГРН', ulacsderg_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДатаПерв__ДатаЗаписи', ulacsderg_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ДержРеестрАО__ОГРН', ulacsderg_info).alias('ogrn').cast('string'),
                                    check_column_exists('ДержРеестрАО__ИНН', ulacsderg_info).alias('inn').cast('string'),
                                    check_column_exists('ДержРеестрАО__НаимЮЛПолн', ulacsderg_info).alias('ulfull_name').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДата__ГРН', ulacsderg_info).alias('egrul_dop_num').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДата__ДатаЗаписи', ulacsderg_info).alias('egrul_dop_date').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДатаИспр__ГРН', ulacsderg_info).alias('egrul_teh_dop_num').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДатаИспр__ДатаЗаписи', ulacsderg_info).alias('egrul_teh_dop_date').cast('string'),
                                    check_column_exists('ДержРеестрАО__ОГРН', ulacsderg_info).alias('ogrn_reestr').cast('string'),
                                    check_column_exists('ДержРеестрАО__ИНН', ulacsderg_info).alias('inn_reestr').cast('string'),
                                    check_column_exists('ДержРеестрАО__НаимЮЛПолн', ulacsderg_info).alias('fullname_reestr').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДата__ГРН', ulacsderg_info).alias('egrul_num_reestr').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДата__ДатаЗаписи', ulacsderg_info).alias('egrul_date_reestr').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДатаИспр__ГРН', ulacsderg_info).alias('egrul_teh_num_reestr').cast('string'),
                                    check_column_exists('ДержРеестрАО_ГРНДатаИспр__ДатаЗаписи', ulacsderg_info).alias('egrul_teh_date_reestr').cast('string'),
                                    ).withColumn('table_guid', f.expr("uuid()")) \
                                    .withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', '')) \
                                    .withColumn('fullname_reestr', f.regexp_replace(f.col('fullname_reestr'), '[\n\r\t]', '')) \


    return ulacsderg_info


def create_ulokved_info(df):
    ulokved_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    f.col('СвОКВЭД.*')))

    ulokved_info = ulokved_info.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвОКВЭДОсн__КодОКВЭД', ulokved_info).alias('okved_num').cast('string'),
                                    check_column_exists('СвОКВЭДОсн__НаимОКВЭД', ulokved_info).alias('okved_name').cast('string'),
                                    check_column_exists('СвОКВЭДОсн__ПрВерсОКВЭД', ulokved_info).alias('okved_ver').cast('string'),
                                    check_column_exists('СвОКВЭДОсн_ГРНДата__ГРН', ulokved_info).alias('egrul_num').cast('string'),
                                    check_column_exists('СвОКВЭДОсн_ГРНДата__ДатаЗаписи', ulokved_info).alias('egrul_date').cast('string'),
                                    check_column_exists('СвОКВЭДОсн_ГРНДатаИспр__ГРН', ulokved_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('СвОКВЭДОсн_ГРНДатаИспр__ДатаЗаписи', ulokved_info).alias('egrul_teh_date').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп__КодОКВЭД', ulokved_info).alias('okved2_num').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп__НаимОКВЭД', ulokved_info).alias('okved2_name').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп__ПрВерсОКВЭД', ulokved_info).alias('okved2_ver').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп_ГРНДата__ГРН', ulokved_info).alias('egrul2_num').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп_ГРНДата__ДатаЗаписи', ulokved_info).alias('egrul2_date').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп_ГРНДатаИспр__ГРН', ulokved_info).alias('egrul2_teh_num').cast('string'),
                                    check_column_exists('_СвОКВЭДДоп_ГРНДатаИспр__ДатаЗаписи', ulokved_info).alias('egrul2_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))
    return ulokved_info


# Проверить на старой схеме СвЛицензия.* или СвЛицензия
# Может быть и массивом и нет
def create_ullicenses_info(df):
    ullicenses_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвЛицензия', df)))

    if 'СвЛицензия' in get_arrays(df.select(check_column_exists('СвЛицензия', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_СвЛицензия__СерЛиц', ullicenses_info).alias('lic_ser').cast('string'),
                                    check_column_exists('_СвЛицензия__НомЛиц', ullicenses_info).alias('lic_num').cast('string'),
                                    check_column_exists('_СвЛицензия__ВидЛиц', ullicenses_info).alias('lic_type').cast('string'),
                                    check_column_exists('_СвЛицензия__ДатаЛиц', ullicenses_info).alias('lic_date').cast('string'),
                                    check_column_exists('_СвЛицензия__ДатаНачЛиц', ullicenses_info).alias('licstart_date').cast('string'),
                                    check_column_exists('_СвЛицензия__ДатаОкончЛиц', ullicenses_info).alias('licend_date').cast('string'),
                                    check_column_exists('_СвЛицензия_НаимЛицВидДеят', ullicenses_info).alias('lic_name').cast('string'),
                                    check_column_exists('_СвЛицензия_МестоДейстЛиц', ullicenses_info).alias('lic_place').cast('string'),
                                    check_column_exists('_СвЛицензия_ЛицОргВыдЛиц', ullicenses_info).alias('lic_org').cast('string'),
                                    check_column_exists('_СвЛицензия_ГРНДата__ГРН', ullicenses_info).alias('egrul_num').cast('string'),
                                    check_column_exists('_СвЛицензия_ГРНДата__ДатаЗаписи', ullicenses_info).alias('egrul_date').cast('string'),
                                    check_column_exists('_СвЛицензия_ГРНДатаИспр__ГРН', ullicenses_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('_СвЛицензия_ГРНДатаИспр__ДатаЗаписи', ullicenses_info).alias('egrul_teh_date').cast('string'),
                                    check_column_exists('_СвЛицензия_СвПриостЛиц__ДатаПриостЛиц', ullicenses_info).alias('licstop_date').cast('string'),
                                    check_column_exists('_СвЛицензия_СвПриостЛиц__ЛицОргПриостЛиц', ullicenses_info).alias('licstop_org').cast('string'),
                                    check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНДата__ГРН', ullicenses_info).alias('egrul_dop_num').cast('string'),
                                    check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНДата__ДатаЗаписи', ullicenses_info).alias('egrul_dop_date').cast('string'),
                                    check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНДатаИспр__ГРН', ullicenses_info).alias('egrul_teh_dop_num').cast('string'),
                                    check_column_exists('_СвЛицензия_СвПриостЛиц_ГРНДатаИспр__ДатаЗаписи', ullicenses_info).alias('egrul_teh_dop_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвЛицензия__СерЛиц', ullicenses_info).alias('lic_ser').cast('string'),
                                    check_column_exists('СвЛицензия__НомЛиц', ullicenses_info).alias('lic_num').cast('string'),
                                    check_column_exists('СвЛицензия__ВидЛиц', ullicenses_info).alias('lic_type').cast('string'),
                                    check_column_exists('СвЛицензия__ДатаЛиц', ullicenses_info).alias('lic_date').cast('string'),
                                    check_column_exists('СвЛицензия__ДатаНачЛиц', ullicenses_info).alias('licstart_date').cast('string'),
                                    check_column_exists('СвЛицензия__ДатаОкончЛиц', ullicenses_info).alias('licend_date').cast('string'),
                                    check_column_exists('СвЛицензия_НаимЛицВидДеят', ullicenses_info).alias('lic_name').cast('string'),
                                    check_column_exists('СвЛицензия_МестоДейстЛиц', ullicenses_info).alias('lic_place').cast('string'),
                                    check_column_exists('СвЛицензия_ЛицОргВыдЛиц', ullicenses_info).alias('lic_org').cast('string'),
                                    check_column_exists('СвЛицензия_ГРНДата__ГРН', ullicenses_info).alias('egrul_num').cast('string'),
                                    check_column_exists('СвЛицензия_ГРНДата__ДатаЗаписи', ullicenses_info).alias('egrul_date').cast('string'),
                                    check_column_exists('СвЛицензия_ГРНДатаИспр__ГРН', ullicenses_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('СвЛицензия_ГРНДатаИспр__ДатаЗаписи', ullicenses_info).alias('egrul_teh_date').cast('string'),
                                    check_column_exists('СвЛицензия_СвПриостЛиц__ДатаПриостЛиц', ullicenses_info).alias('licstop_date').cast('string'),
                                    check_column_exists('СвЛицензия_СвПриостЛиц__ЛицОргПриостЛиц', ullicenses_info).alias('licstop_org').cast('string'),
                                    check_column_exists('СвЛицензия_СвПриостЛиц_ГРНДата__ГРН', ullicenses_info).alias('egrul_dop_num').cast('string'),
                                    check_column_exists('СвЛицензия_СвПриостЛиц_ГРНДата__ДатаЗаписи', ullicenses_info).alias('egrul_dop_date').cast('string'),
                                    check_column_exists('СвЛицензия_СвПриостЛиц_ГРНДатаИспр__ГРН', ullicenses_info).alias('egrul_teh_dop_num').cast('string'),
                                    check_column_exists('СвЛицензия_СвПриостЛиц_ГРНДатаИспр__ДатаЗаписи', ullicenses_info).alias('egrul_teh_dop_date').cast('string')]

    ullicenses_info = ullicenses_info.select(*cols).withColumn('table_guid', f.expr("uuid()"))
    return ullicenses_info

# Может быть массивом может быть и нет
def create_ulfilial_info(df):
    ulfilial_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвПодразд.*', df)))

    if 'СвФилиал' in get_arrays(df.select(check_column_exists('СвПодразд.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                         f.col('load_date'),
                                         f.col('folder_date'),
                                         check_column_exists('_СвФилиал_ОгрДосСв__ОгрДосСв', ulfilial_info).alias(
                                             'filial_limit_info').cast('string'),
                                         check_column_exists('_СвФилиал_ОгрДосСв_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul_num_limit').cast('string'),
                                         check_column_exists('_СвФилиал_ОгрДосСв_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_date_limit').cast('string'),
                                         check_column_exists('_СвФилиал_ОгрДосСв_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul_teh_num_limit').cast('string'),
                                         check_column_exists('_СвФилиал_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_teh_date_limit').cast(
                                             'string'),
                                         check_column_exists('_СвФилиал_ГРНДатаПерв__ГРН', ulfilial_info).alias(
                                             'egrul_num').cast('string'),
                                         check_column_exists('_СвФилиал_ГРНДатаПерв__ДатаЗаписи', ulfilial_info).alias(
                                             'egrul_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвНаим__НаимПолн', ulfilial_info).alias(
                                             'full_name').cast('string'),
                                         check_column_exists('_СвФилиал_СвНаим_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul2_num').cast('string'),
                                         check_column_exists('_СвФилиал_СвНаим_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul2_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвНаим_ГРНДатаИспр__ГРН', ulfilial_info).alias(
                                             'egrul2_teh_num').cast('string'),
                                         check_column_exists('_СвФилиал_СвНаим_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul2_teh_date').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ__Индекс', ulfilial_info).alias(
                                             'index').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ__КодРегион', ulfilial_info).alias(
                                             'region_code').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ__КодАдрКладр', ulfilial_info).alias(
                                             'cladr_code').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ__Дом', ulfilial_info).alias(
                                             'home').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ__Корпус', ulfilial_info).alias(
                                             'corps').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ__Кварт', ulfilial_info).alias(
                                             'kvart').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_НаселПункт__НаимНаселПункт',
                                                             ulfilial_info).alias('region_type').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_НаселПункт__ТипНаселПункт',
                                                             ulfilial_info).alias('region_name').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Район__ТипРайон', ulfilial_info).alias(
                                             'district_type').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Район__НаимРайон', ulfilial_info).alias(
                                             'district_name').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Регион__ТипРегион',
                                                             ulfilial_info).alias('city_type').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Регион__НаимРегион',
                                                             ulfilial_info).alias('city_name').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Улица__ТипУлица', ulfilial_info).alias(
                                             'locality_type').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Улица__НаимУлица', ulfilial_info).alias(
                                             'locality_name').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Город__ТипГород', ulfilial_info).alias(
                                             'street_type').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_Город__НаимГород', ulfilial_info).alias(
                                             'street_name').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul3_num').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul3_date').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_ГРНДатаИспр__ГРН', ulfilial_info).alias(
                                             'egrul3_teh_num').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНРФ_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul3_teh_date').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС__ИдНом', ulfilial_info).alias(
                                             'id_gar_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС__Индекс', ulfilial_info).alias(
                                             'index_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_Регион', ulfilial_info).alias(
                                             'region_code_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_НаимРегион', ulfilial_info).alias(
                                             'region_name_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_МуниципРайон__ВидКод',
                                                             ulfilial_info).alias('municip_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_МуниципРайон__Наим',
                                                             ulfilial_info).alias('municip_name_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ГородСелПоселен__ВидКод',
                                                             ulfilial_info).alias('posel_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ГородСелПоселен__Наим',
                                                             ulfilial_info).alias('posel_name_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_НаселенПункт__Вид',
                                                             ulfilial_info).alias('city_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_НаселенПункт__Наим',
                                                             ulfilial_info).alias('city_name_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ЭлПланСтруктур__Тип',
                                                             ulfilial_info).alias('elplan_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ЭлПланСтруктур__Наим',
                                                             ulfilial_info).alias('elplan_name_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ЭлУлДорСети__Тип',
                                                             ulfilial_info).alias('eldor_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ЭлУлДорСети__Наим',
                                                             ulfilial_info).alias('eldor_name_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_Здание__Тип', ulfilial_info).alias(
                                             'build_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_Здание__Номер', ulfilial_info).alias(
                                             'build_num_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ПомещЗдания__Тип',
                                                             ulfilial_info).alias('pom_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ПомещЗдания__Номер',
                                                             ulfilial_info).alias('pom_num_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ПомещКвартиры__Тип',
                                                             ulfilial_info).alias('room_type_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ПомещКвартиры__Номер',
                                                             ulfilial_info).alias('room_num_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul_num_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_date_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul_teh_num_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНФИАС_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_teh_date_fias').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн__ОКСМ', ulfilial_info).alias(
                                             'oksm').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн__НаимСтран', ulfilial_info).alias(
                                             'country_name').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн__АдрИн', ulfilial_info).alias(
                                             'in_address').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul4_num').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul4_date').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн_ГРНДатаИспр__ГРН', ulfilial_info).alias(
                                             'egrul4_teh_num').cast('string'),
                                         check_column_exists('_СвФилиал_АдрМНИн_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul4_teh_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал__КПП', ulfilial_info).alias(
                                             'kpp').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал__ДатаПостУч',
                                                             ulfilial_info).alias('uch_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО__КодНО',
                                                             ulfilial_info).alias('no_code').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО__НаимНО',
                                                             ulfilial_info).alias('no_name').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО__НомТел',
                                                             ulfilial_info).alias('tel_number').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО_ГРНДата__ГРН',
                                                             ulfilial_info).alias('egrul5_num').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul5_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul5_teh_num').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul5_teh_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_ГРНДата__ГРН',
                                                             ulfilial_info).alias('egrul6_num').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul6_date').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul6_teh_num').cast('string'),
                                         check_column_exists('_СвФилиал_СвУчетНОФилиал_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul6_teh_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                         f.col('load_date'),
                                         f.col('folder_date'),
                                         check_column_exists('СвФилиал_ОгрДосСв__ОгрДосСв', ulfilial_info).alias(
                                             'filial_limit_info').cast('string'),
                                         check_column_exists('СвФилиал_ОгрДосСв_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul_num_limit').cast('string'),
                                         check_column_exists('СвФилиал_ОгрДосСв_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_date_limit').cast('string'),
                                         check_column_exists('СвФилиал_ОгрДосСв_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul_teh_num_limit').cast('string'),
                                         check_column_exists('СвФилиал_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_teh_date_limit').cast(
                                             'string'),
                                         check_column_exists('СвФилиал_ГРНДатаПерв__ГРН', ulfilial_info).alias(
                                             'egrul_num').cast('string'),
                                         check_column_exists('СвФилиал_ГРНДатаПерв__ДатаЗаписи', ulfilial_info).alias(
                                             'egrul_date').cast('string'),
                                         check_column_exists('СвФилиал_СвНаим__НаимПолн', ulfilial_info).alias(
                                             'full_name').cast('string'),
                                         check_column_exists('СвФилиал_СвНаим_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul2_num').cast('string'),
                                         check_column_exists('СвФилиал_СвНаим_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul2_date').cast('string'),
                                         check_column_exists('СвФилиал_СвНаим_ГРНДатаИспр__ГРН', ulfilial_info).alias(
                                             'egrul2_teh_num').cast('string'),
                                         check_column_exists('СвФилиал_СвНаим_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul2_teh_date').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ__Индекс', ulfilial_info).alias(
                                             'index').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ__КодРегион', ulfilial_info).alias(
                                             'region_code').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ__КодАдрКладр', ulfilial_info).alias(
                                             'cladr_code').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ__Дом', ulfilial_info).alias(
                                             'home').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ__Корпус', ulfilial_info).alias(
                                             'corps').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ__Кварт', ulfilial_info).alias(
                                             'kvart').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_НаселПункт__НаимНаселПункт',
                                                             ulfilial_info).alias('region_type').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_НаселПункт__ТипНаселПункт',
                                                             ulfilial_info).alias('region_name').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Район__ТипРайон', ulfilial_info).alias(
                                             'district_type').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Район__НаимРайон', ulfilial_info).alias(
                                             'district_name').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Регион__ТипРегион',
                                                             ulfilial_info).alias('city_type').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Регион__НаимРегион',
                                                             ulfilial_info).alias('city_name').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Улица__ТипУлица', ulfilial_info).alias(
                                             'locality_type').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Улица__НаимУлица', ulfilial_info).alias(
                                             'locality_name').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Город__ТипГород', ulfilial_info).alias(
                                             'street_type').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_Город__НаимГород', ulfilial_info).alias(
                                             'street_name').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul3_num').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul3_date').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_ГРНДатаИспр__ГРН', ulfilial_info).alias(
                                             'egrul3_teh_num').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНРФ_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul3_teh_date').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС__ИдНом', ulfilial_info).alias(
                                             'id_gar_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС__Индекс', ulfilial_info).alias(
                                             'index_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_Регион', ulfilial_info).alias(
                                             'region_code_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_НаимРегион', ulfilial_info).alias(
                                             'region_name_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_МуниципРайон__ВидКод',
                                                             ulfilial_info).alias('municip_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_МуниципРайон__Наим',
                                                             ulfilial_info).alias('municip_name_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ГородСелПоселен__ВидКод',
                                                             ulfilial_info).alias('posel_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ГородСелПоселен__Наим',
                                                             ulfilial_info).alias('posel_name_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_НаселенПункт__Вид',
                                                             ulfilial_info).alias('city_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_НаселенПункт__Наим',
                                                             ulfilial_info).alias('city_name_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ЭлПланСтруктур__Тип',
                                                             ulfilial_info).alias('elplan_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ЭлПланСтруктур__Наим',
                                                             ulfilial_info).alias('elplan_name_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ЭлУлДорСети__Тип',
                                                             ulfilial_info).alias('eldor_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ЭлУлДорСети__Наим',
                                                             ulfilial_info).alias('eldor_name_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_Здание__Тип', ulfilial_info).alias(
                                             'build_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_Здание__Номер', ulfilial_info).alias(
                                             'build_num_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ПомещЗдания__Тип',
                                                             ulfilial_info).alias('pom_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ПомещЗдания__Номер',
                                                             ulfilial_info).alias('pom_num_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ПомещКвартиры__Тип',
                                                             ulfilial_info).alias('room_type_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ПомещКвартиры__Номер',
                                                             ulfilial_info).alias('room_num_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul_num_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_date_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul_teh_num_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНФИАС_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul_teh_date_fias').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн__ОКСМ', ulfilial_info).alias(
                                             'oksm').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн__НаимСтран', ulfilial_info).alias(
                                             'country_name').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн__АдрИн', ulfilial_info).alias(
                                             'in_address').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн_ГРНДата__ГРН', ulfilial_info).alias(
                                             'egrul4_num').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul4_date').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн_ГРНДатаИспр__ГРН', ulfilial_info).alias(
                                             'egrul4_teh_num').cast('string'),
                                         check_column_exists('СвФилиал_АдрМНИн_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul4_teh_date').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал__КПП', ulfilial_info).alias(
                                             'kpp').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал__ДатаПостУч',
                                                             ulfilial_info).alias('uch_date').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО__КодНО',
                                                             ulfilial_info).alias('no_code').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО__НаимНО',
                                                             ulfilial_info).alias('no_name').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО__НомТел',
                                                             ulfilial_info).alias('tel_number').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО_ГРНДата__ГРН',
                                                             ulfilial_info).alias('egrul5_num').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul5_date').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul5_teh_num').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul5_teh_date').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_ГРНДата__ГРН',
                                                             ulfilial_info).alias('egrul6_num').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_ГРНДата__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul6_date').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_ГРНДатаИспр__ГРН',
                                                             ulfilial_info).alias('egrul6_teh_num').cast('string'),
                                         check_column_exists('СвФилиал_СвУчетНОФилиал_ГРНДатаИспр__ДатаЗаписи',
                                                             ulfilial_info).alias('egrul6_teh_date').cast('string')]

    ulfilial_info = ulfilial_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('full_name', f.regexp_replace(f.col('full_name'), '[\n\r\t]', ''))

    return ulfilial_info


def create_ulrepresent_info(df):
    ulrepresent_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвПодразд.*', df)))

    if 'СвПредстав' in get_arrays(df.select(check_column_exists('СвПодразд.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_СвПредстав_ОгрДосСв__ОгрДосСв', ulrepresent_info).alias('dep_limit_info').cast('string'),
                                    check_column_exists('_СвПредстав_ОгрДосСв_ГРНДата__ГРН', ulrepresent_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('_СвПредстав_ОгрДосСв_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('_СвПредстав_ОгрДосСв_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('_СвПредстав_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('_СвПредстав_ГРНДатаПерв__ГРН', ulrepresent_info).alias('egrul_numB').cast('string'),
                                    check_column_exists('_СвПредстав_ГРНДатаПерв__ДатаЗаписи', ulrepresent_info).alias('egrul_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвНаим__НаимПолн', ulrepresent_info).alias('full_nameB').cast('string'),
                                    check_column_exists('_СвПредстав_СвНаим_ГРНДата__ГРН', ulrepresent_info).alias('egrul2_numB').cast('string'),
                                    check_column_exists('_СвПредстав_СвНаим_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul2_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвНаим_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul2_teh_numB').cast('string'),
                                    check_column_exists('_СвПредстав_СвНаим_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul2_teh_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ__Индекс', ulrepresent_info).alias('indexB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ__КодРегион', ulrepresent_info).alias('region_codeB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ__КодАдрКладр', ulrepresent_info).alias('cladr_codeB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ__Дом', ulrepresent_info).alias('homeB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ__Корпус', ulrepresent_info).alias('corpsB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ__Кварт', ulrepresent_info).alias('kvartB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Город__НаимГород', ulrepresent_info).alias('city_name').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Город__ТипГород', ulrepresent_info).alias('city_code').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_НаселПункт__НаимНаселПункт', ulrepresent_info).alias('town_name').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_НаселПункт__ТипНаселПункт', ulrepresent_info).alias('town_code').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Район__НаимРайон', ulrepresent_info).alias('district_name').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Район__ТипРайон', ulrepresent_info).alias('district_code').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Регион__НаимРегион', ulrepresent_info).alias('region_name').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Регион__ТипРегион', ulrepresent_info).alias('region_code').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Улица__НаимУлица', ulrepresent_info).alias('street_name').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_Улица__ТипУлица', ulrepresent_info).alias('street_code').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_ГРНДата__ГРН', ulrepresent_info).alias('egrul3_numB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul3_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul3_teh_numB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНРФ_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul3_teh_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС__ИдНом', ulrepresent_info).alias('id_gar_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС__Индекс', ulrepresent_info).alias('index_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_Регион', ulrepresent_info).alias('region_code_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_НаимРегион', ulrepresent_info).alias('region_name_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_МуниципРайон__ВидКод', ulrepresent_info).alias('municip_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_МуниципРайон__Наим', ulrepresent_info).alias('municip_name_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ГородСелПоселен__ВидКод', ulrepresent_info).alias('posel_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ГородСелПоселен__Наим', ulrepresent_info).alias('posel_name_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_НаселенПункт__Вид', ulrepresent_info).alias('city_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_НаселенПункт__Наим', ulrepresent_info).alias('city_name_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ЭлПланСтруктур__Тип', ulrepresent_info).alias('elplan_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ЭлПланСтруктур__Наим', ulrepresent_info).alias('elplan_name_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ЭлУлДорСети__Тип', ulrepresent_info).alias('eldor_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ЭлУлДорСети__Наим', ulrepresent_info).alias('eldor_name_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_Здание__Тип', ulrepresent_info).alias('build_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_Здание__Номер', ulrepresent_info).alias('build_num_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ПомещЗдания__Тип', ulrepresent_info).alias('pom_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ПомещЗдания__Номер', ulrepresent_info).alias('pom_num_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ПомещКвартиры__Тип', ulrepresent_info).alias('room_type_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ПомещКвартиры__Номер', ulrepresent_info).alias('room_num_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ГРНДата__ГРН', ulrepresent_info).alias('egrul_num_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul_date_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul_teh_num_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНФИАС_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul_teh_date_fias').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн__ОКСМ', ulrepresent_info).alias('oksmB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн__НаимСтран', ulrepresent_info).alias('country_nameB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн__АдрИн', ulrepresent_info).alias('in_addressB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн_ГРНДата__ГРН', ulrepresent_info).alias('egrul4_numB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul4_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul4_teh_numB').cast('string'),
                                    check_column_exists('_СвПредстав_АдрМНИн_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul4_teh_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал__КПП', ulrepresent_info).alias('kppB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал__ДатаПостУч', ulrepresent_info).alias('uch_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_СвНО__НомТел', ulrepresent_info).alias('tel_numberB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_СвНО_ГРНДата__ГРН', ulrepresent_info).alias('egrul5_numB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_СвНО_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul5_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul5_teh_numB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul5_teh_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_ГРНДата__ГРН', ulrepresent_info).alias('egrul6_numB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul6_dateB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul6_teh_numB').cast('string'),
                                    check_column_exists('_СвПредстав_СвУчетНОФилиал_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul6_teh_dateB').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвПредстав_ОгрДосСв__ОгрДосСв', ulrepresent_info).alias('dep_limit_info').cast('string'),
                                    check_column_exists('СвПредстав_ОгрДосСв_ГРНДата__ГРН', ulrepresent_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('СвПредстав_ОгрДосСв_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('СвПредстав_ОгрДосСв_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('СвПредстав_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СвПредстав_ГРНДатаПерв__ГРН', ulrepresent_info).alias('egrul_numB').cast('string'),
                                    check_column_exists('СвПредстав_ГРНДатаПерв__ДатаЗаписи', ulrepresent_info).alias('egrul_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвНаим__НаимПолн', ulrepresent_info).alias('full_nameB').cast('string'),
                                    check_column_exists('СвПредстав_СвНаим_ГРНДата__ГРН', ulrepresent_info).alias('egrul2_numB').cast('string'),
                                    check_column_exists('СвПредстав_СвНаим_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul2_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвНаим_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul2_teh_numB').cast('string'),
                                    check_column_exists('СвПредстав_СвНаим_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul2_teh_dateB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ__Индекс', ulrepresent_info).alias('indexB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ__КодРегион', ulrepresent_info).alias('region_codeB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ__КодАдрКладр', ulrepresent_info).alias('cladr_codeB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ__Дом', ulrepresent_info).alias('homeB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ__Корпус', ulrepresent_info).alias('corpsB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ__Кварт', ulrepresent_info).alias('kvartB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Город__НаимГород', ulrepresent_info).alias('city_name').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Город__ТипГород', ulrepresent_info).alias('city_code').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_НаселПункт__НаимНаселПункт', ulrepresent_info).alias('town_name').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_НаселПункт__ТипНаселПункт', ulrepresent_info).alias('town_code').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Район__НаимРайон', ulrepresent_info).alias('district_name').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Район__ТипРайон', ulrepresent_info).alias('district_code').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Регион__НаимРегион', ulrepresent_info).alias('region_name').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Регион__ТипРегион', ulrepresent_info).alias('region_code').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Улица__НаимУлица', ulrepresent_info).alias('street_name').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_Улица__ТипУлица', ulrepresent_info).alias('street_code').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_ГРНДата__ГРН', ulrepresent_info).alias('egrul3_numB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul3_dateB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul3_teh_numB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНРФ_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul3_teh_dateB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС__ИдНом', ulrepresent_info).alias('id_gar_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС__Индекс', ulrepresent_info).alias('index_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_Регион', ulrepresent_info).alias('region_code_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_НаимРегион', ulrepresent_info).alias('region_name_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_МуниципРайон__ВидКод', ulrepresent_info).alias('municip_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_МуниципРайон__Наим', ulrepresent_info).alias('municip_name_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ГородСелПоселен__ВидКод', ulrepresent_info).alias('posel_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ГородСелПоселен__Наим', ulrepresent_info).alias('posel_name_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_НаселенПункт__Вид', ulrepresent_info).alias('city_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_НаселенПункт__Наим', ulrepresent_info).alias('city_name_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ЭлПланСтруктур__Тип', ulrepresent_info).alias('elplan_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ЭлПланСтруктур__Наим', ulrepresent_info).alias('elplan_name_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ЭлУлДорСети__Тип', ulrepresent_info).alias('eldor_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ЭлУлДорСети__Наим', ulrepresent_info).alias('eldor_name_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_Здание__Тип', ulrepresent_info).alias('build_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_Здание__Номер', ulrepresent_info).alias('build_num_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ПомещЗдания__Тип', ulrepresent_info).alias('pom_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ПомещЗдания__Номер', ulrepresent_info).alias('pom_num_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ПомещКвартиры__Тип', ulrepresent_info).alias('room_type_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ПомещКвартиры__Номер', ulrepresent_info).alias('room_num_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ГРНДата__ГРН', ulrepresent_info).alias('egrul_num_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul_date_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul_teh_num_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНФИАС_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul_teh_date_fias').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн__ОКСМ', ulrepresent_info).alias('oksmB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн__НаимСтран', ulrepresent_info).alias('country_nameB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн__АдрИн', ulrepresent_info).alias('in_addressB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн_ГРНДата__ГРН', ulrepresent_info).alias('egrul4_numB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul4_dateB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul4_teh_numB').cast('string'),
                                    check_column_exists('СвПредстав_АдрМНИн_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul4_teh_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал__КПП', ulrepresent_info).alias('kppB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал__ДатаПостУч', ulrepresent_info).alias('uch_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_СвНО__НомТел', ulrepresent_info).alias('tel_numberB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_СвНО_ГРНДата__ГРН', ulrepresent_info).alias('egrul5_numB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_СвНО_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul5_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul5_teh_numB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_СвНО_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul5_teh_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_ГРНДата__ГРН', ulrepresent_info).alias('egrul6_numB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_ГРНДата__ДатаЗаписи', ulrepresent_info).alias('egrul6_dateB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_ГРНДатаИспр__ГРН', ulrepresent_info).alias('egrul6_teh_numB').cast('string'),
                                    check_column_exists('СвПредстав_СвУчетНОФилиал_ГРНДатаИспр__ДатаЗаписи', ulrepresent_info).alias('egrul6_teh_dateB').cast('string')]

    ulrepresent_info = ulrepresent_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('full_nameB', f.regexp_replace(f.col('full_nameB'), '[\n\r\t]', ''))
    return ulrepresent_info


def create_ulreorg_info(df):
    ulreorg_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвРеорг.*', df)))

    if 'СвРеоргЮЛ' in get_arrays(df.select(check_column_exists('СвРеорг.*', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('ОгрДосСв__ОгрДосСв', ulreorg_info).alias('reorg_limit_info').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДата__ГРН', ulreorg_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДата__ДатаЗаписи', ulreorg_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДатаИспр__ГРН', ulreorg_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulreorg_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СвСтатус__КодСтатусЮЛ', ulreorg_info).alias('ulstatus_code').cast('string'),
                                    check_column_exists('СвСтатус__НаимСтатусЮЛ', ulreorg_info).alias('ulstatus_name').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulreorg_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulreorg_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИзмСостРеоргЮЛ__ГРН', ulreorg_info).alias('egrul_teh2_num').cast('string'),
                                    check_column_exists('ГРНДатаИзмСостРеоргЮЛ__ДатаЗаписи', ulreorg_info).alias('egrul_teh2_date').cast('string'),
                                    check_column_exists('_СвРеоргЮЛ__ОГРН', ulreorg_info).alias('ogrn').cast('string'),
                                    check_column_exists('_СвРеоргЮЛ__ИНН', ulreorg_info).alias('inn').cast('string'),
                                    check_column_exists('_СвРеоргЮЛ__НаимЮЛПолн', ulreorg_info).alias('ulfull_name').cast('string'),
                                    check_column_exists('_СвРеоргЮЛ__СостЮЛпосле', ulreorg_info).alias('ul_status').cast('string'),
                                    check_column_exists('_СвРеоргЮЛ_ГРНДатаИспр__ГРН', ulreorg_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('_СвРеоргЮЛ_ГРНДатаИспр__ДатаЗаписи', ulreorg_info).alias('egrul_teh_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('ОгрДосСв__ОгрДосСв', ulreorg_info).alias('reorg_limit_info').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДата__ГРН', ulreorg_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДата__ДатаЗаписи', ulreorg_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДатаИспр__ГРН', ulreorg_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulreorg_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СвСтатус__КодСтатусЮЛ', ulreorg_info).alias('ulstatus_code').cast('string'),
                                    check_column_exists('СвСтатус__НаимСтатусЮЛ', ulreorg_info).alias('ulstatus_name').cast('string'),
                                    check_column_exists('ГРНДата__ГРН', ulreorg_info).alias('egrul_num').cast('string'),
                                    check_column_exists('ГРНДата__ДатаЗаписи', ulreorg_info).alias('egrul_date').cast('string'),
                                    check_column_exists('ГРНДатаИзмСостРеоргЮЛ__ГРН', ulreorg_info).alias('egrul_teh2_num').cast('string'),
                                    check_column_exists('ГРНДатаИзмСостРеоргЮЛ__ДатаЗаписи', ulreorg_info).alias('egrul_teh2_date').cast('string'),
                                    check_column_exists('СвРеоргЮЛ__ОГРН', ulreorg_info).alias('ogrn').cast('string'),
                                    check_column_exists('СвРеоргЮЛ__ИНН', ulreorg_info).alias('inn').cast('string'),
                                    check_column_exists('СвРеоргЮЛ__НаимЮЛПолн', ulreorg_info).alias('ulfull_name').cast('string'),
                                    check_column_exists('СвРеоргЮЛ__СостЮЛпосле', ulreorg_info).alias('ul_status').cast('string'),
                                    check_column_exists('СвРеоргЮЛ_ГРНДатаИспр__ГРН', ulreorg_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('СвРеоргЮЛ_ГРНДатаИспр__ДатаЗаписи', ulreorg_info).alias('egrul_teh_date').cast('string')]

    ulreorg_info = ulreorg_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', ''))
    return ulreorg_info

# может быть массивом, а может быть и нет
def create_pravopred_info(df):
    pravopred_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвПредш', df)))

    if 'СвПредш' in get_arrays(df.select(check_column_exists('СвПредш', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_СвПредш__ОГРН', pravopred_info).alias('ogrn').cast('string'),
                                    check_column_exists('_СвПредш__ИНН', pravopred_info).alias('inn').cast('string'),
                                    check_column_exists('_СвПредш__НаимЮЛПолн', pravopred_info).alias('ulname_full').cast('string'),
                                    check_column_exists('_СвПредш_СвЮЛсложнРеорг__ОГРН', pravopred_info).alias('ogrn2').cast('string'),
                                    check_column_exists('_СвПредш_СвЮЛсложнРеорг__ИНН', pravopred_info).alias('inn2').cast('string'),
                                    check_column_exists('_СвПредш_ОгрДосСв__ОгрДосСв', pravopred_info).alias('pred_limit_info').cast('string'),
                                    check_column_exists('_СвПредш_ОгрДосСв_ГРНДата__ГРН', pravopred_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('_СвПредш_ОгрДосСв_ГРНДата__ДатаЗаписи', pravopred_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('_СвПредш_ОгрДосСв_ГРНДатаИспр__ГРН', pravopred_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('_СвПредш_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', pravopred_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('_СвПредш_СвЮЛсложнРеорг__НаимЮЛПолн', pravopred_info).alias('ulname2_full').cast('string'),
                                    check_column_exists('_СвПредш_ГРНДата__ГРН', pravopred_info).alias('egrul_num').cast('string'),
                                    check_column_exists('_СвПредш_ГРНДата__ДатаЗаписи', pravopred_info).alias('egrul_date').cast('string'),
                                    check_column_exists('_СвПредш_ГРНДатаИспр__ГРН', pravopred_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('_СвПредш_ГРНДатаИспр__ДатаЗаписи', pravopred_info).alias('egrul_teh_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвПредш__ОГРН', pravopred_info).alias('ogrn').cast('string'),
                                    check_column_exists('СвПредш__ИНН', pravopred_info).alias('inn').cast('string'),
                                    check_column_exists('СвПредш__НаимЮЛПолн', pravopred_info).alias('ulname_full').cast('string'),
                                    check_column_exists('СвПредш_СвЮЛсложнРеорг__ОГРН', pravopred_info).alias('ogrn2').cast('string'),
                                    check_column_exists('СвПредш_СвЮЛсложнРеорг__ИНН', pravopred_info).alias('inn2').cast('string'),
                                    check_column_exists('СвПредш_ОгрДосСв__ОгрДосСв', pravopred_info).alias('pred_limit_info').cast('string'),
                                    check_column_exists('СвПредш_ОгрДосСв_ГРНДата__ГРН', pravopred_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('СвПредш_ОгрДосСв_ГРНДата__ДатаЗаписи', pravopred_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('СвПредш_ОгрДосСв_ГРНДатаИспр__ГРН', pravopred_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('СвПредш_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', pravopred_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СвПредш_СвЮЛсложнРеорг__НаимЮЛПолн', pravopred_info).alias('ulname2_full').cast('string'),
                                    check_column_exists('СвПредш_ГРНДата__ГРН', pravopred_info).alias('egrul_num').cast('string'),
                                    check_column_exists('СвПредш_ГРНДата__ДатаЗаписи', pravopred_info).alias('egrul_date').cast('string'),
                                    check_column_exists('СвПредш_ГРНДатаИспр__ГРН', pravopred_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('СвПредш_ГРНДатаИспр__ДатаЗаписи', pravopred_info).alias('egrul_teh_date').cast('string')]

    pravopred_info = pravopred_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('ulname_full', f.regexp_replace(f.col('ulname_full'), '[\n\r\t]', ''))
    return pravopred_info


def create_krestxoz(df):
    krestxoz = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвКФХПредш', df)))

    krestxoz = krestxoz.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_СвКФХПредш__ОГРНИП', krestxoz).alias('ogrnip').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ__Фамилия', krestxoz).alias('surname').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ__Имя', krestxoz).alias('name').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ__Отчество', krestxoz).alias('panronymic').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ__ИННФЛ', krestxoz).alias('innfl').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ_ГРНДата__ГРН', krestxoz).alias('egrul_num').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ_ГРНДата__ДатаЗаписи', krestxoz).alias('egrul_date').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ_ГРНДатаИспр__ГРН', krestxoz).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('_СвКФХПредш_СвФЛ_ГРНДатаИспр__ДатаЗаписи', krestxoz).alias('egrul_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))
    return krestxoz


#  Проверить на старой схеме СвПреем или СвПреем.*
#  Может быть и массивом тогда _ перед СвПреем, может и без тогда как обычно
def create_ulpreemnik_info(df):
    ulpreemnik_info = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвПреем', df)))

    if 'СвПреем' in get_arrays(df.select(check_column_exists('СвПреем', df)).schema):
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('_СвПреем__ОГРН', ulpreemnik_info).alias('ogrn').cast('string'),
                                    check_column_exists('_СвПреем__ИНН', ulpreemnik_info).alias('inn').cast('string'),
                                    check_column_exists('_СвПреем__НаимЮЛПолн', ulpreemnik_info).alias('ulfull_name').cast('string'),
                                    check_column_exists('_СвПреем_СвЮЛсложнРеорг__ОГРН', ulpreemnik_info).alias('ogrn2').cast('string'),
                                    check_column_exists('_СвПреем_СвЮЛсложнРеорг__ИНН', ulpreemnik_info).alias('inn2').cast('string'),
                                    check_column_exists('_СвПреем_ОгрДосСв__ОгрДосСв', ulpreemnik_info).alias('preem_limit_info').cast('string'),
                                    check_column_exists('_СвПреем_ОгрДосСв_ГРНДата__ГРН', ulpreemnik_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('_СвПреем_ОгрДосСв_ГРНДата__ДатаЗаписи', ulpreemnik_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('_СвПреем_ОгрДосСв_ГРНДатаИспр__ГРН', ulpreemnik_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('_СвПреем_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulpreemnik_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('_СвПреем_СвЮЛсложнРеорг__НаимЮЛПолн', ulpreemnik_info).alias('ulfull_name2').cast('string'),
                                    check_column_exists('_СвПреем_ГРНДата__ГРН', ulpreemnik_info).alias('egrul_num').cast('string'),
                                    check_column_exists('_СвПреем_ГРНДата__ДатаЗаписи', ulpreemnik_info).alias('egrul_date').cast('string'),
                                    check_column_exists('_СвПреем_ГРНДатаИспр__ГРН', ulpreemnik_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('_СвПреем_ГРНДатаИспр__ДатаЗаписи', ulpreemnik_info).alias('egrul_teh_date').cast('string')]
    else:
        cols = [f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвПреем__ОГРН', ulpreemnik_info).alias('ogrn').cast('string'),
                                    check_column_exists('СвПреем__ИНН', ulpreemnik_info).alias('inn').cast('string'),
                                    check_column_exists('СвПреем__НаимЮЛПолн', ulpreemnik_info).alias('ulfull_name').cast('string'),
                                    check_column_exists('СвПреем_СвЮЛсложнРеорг__ОГРН', ulpreemnik_info).alias('ogrn2').cast('string'),
                                    check_column_exists('СвПреем_СвЮЛсложнРеорг__ИНН', ulpreemnik_info).alias('inn2').cast('string'),
                                    check_column_exists('СвПреем_ОгрДосСв__ОгрДосСв', ulpreemnik_info).alias('preem_limit_info').cast('string'),
                                    check_column_exists('СвПреем_ОгрДосСв_ГРНДата__ГРН', ulpreemnik_info).alias('egrul_num_limit').cast('string'),
                                    check_column_exists('СвПреем_ОгрДосСв_ГРНДата__ДатаЗаписи', ulpreemnik_info).alias('egrul_date_limit').cast('string'),
                                    check_column_exists('СвПреем_ОгрДосСв_ГРНДатаИспр__ГРН', ulpreemnik_info).alias('egrul_teh_num_limit').cast('string'),
                                    check_column_exists('СвПреем_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи', ulpreemnik_info).alias('egrul_teh_date_limit').cast('string'),
                                    check_column_exists('СвПреем_СвЮЛсложнРеорг__НаимЮЛПолн', ulpreemnik_info).alias('ulfull_name2').cast('string'),
                                    check_column_exists('СвПреем_ГРНДата__ГРН', ulpreemnik_info).alias('egrul_num').cast('string'),
                                    check_column_exists('СвПреем_ГРНДата__ДатаЗаписи', ulpreemnik_info).alias('egrul_date').cast('string'),
                                    check_column_exists('СвПреем_ГРНДатаИспр__ГРН', ulpreemnik_info).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('СвПреем_ГРНДатаИспр__ДатаЗаписи', ulpreemnik_info).alias('egrul_teh_date').cast('string')]

    ulpreemnik_info = ulpreemnik_info.select(*cols).withColumn('table_guid', f.expr("uuid()")).withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', '')).withColumn('ulfull_name2', f.regexp_replace(f.col('ulfull_name2'), '[\n\r\t]', ''))
    return ulpreemnik_info


def create_ulkvhp(df):
    ulkvhp = spark_read(df.select(
                                    f.col('table_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвКФХПреем', df)))

    ulkvhp = ulkvhp.select(f.col('table_guid').alias('rodtable_guid'),
                                    f.col('load_date'),
                                    f.col('folder_date'),
                                    check_column_exists('СвКФХПреем__ОГРНИП', ulkvhp).alias('ogrnip').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ__Фамилия', ulkvhp).alias('surname').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ__Имя', ulkvhp).alias('name').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ__Отчество', ulkvhp).alias('panronymic').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ__ИННФЛ', ulkvhp).alias('innfl').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ_ГРНДата__ГРН', ulkvhp).alias('egrul_num').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ_ГРНДата__ДатаЗаписи', ulkvhp).alias('egrul_date').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ_ГРНДатаИспр__ГРН', ulkvhp).alias('egrul_teh_num').cast('string'),
                                    check_column_exists('СвКФХПреем_СвФЛ_ГРНДатаИспр__ДатаЗаписи', ulkvhp).alias('egrul_teh_date').cast('string')
                                    ).withColumn('table_guid', f.expr("uuid()"))
    return ulkvhp


def create_ulegrul_info(df):
    ulegrul_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        f.col('СвЗапЕГРЮЛ')))

    ulegrul_info = spark_read(ulegrul_info)

    ulegrul_info = ulegrul_info.select(f.col('table_guid').alias('rodtable_guid'),
                                       f.col('load_date'),
                                       f.col('folder_date'),
                                       check_column_exists('_СвЗапЕГРЮЛ__ИдЗап', ulegrul_info).alias('zap_id').cast(
                                           'string'),
                                       check_column_exists('_СвЗапЕГРЮЛ__ГРН', ulegrul_info).alias('egrul_num').cast(
                                           'string'),
                                       check_column_exists('_СвЗапЕГРЮЛ__ДатаЗап', ulegrul_info).alias(
                                           'egrul_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ВидЗап__КодСПВЗ', ulegrul_info).alias(
                                           'spvz_code').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ВидЗап__НаимВидЗап', ulegrul_info).alias(
                                           'spvz_type').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ОгрДосСв__ОгрДосСв', ulegrul_info).alias(
                                           'zapegrul_limit_info').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ОгрДосСв_ГРНДата__ГРН', ulegrul_info).alias(
                                           'egrul_num_limit').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ОгрДосСв_ГРНДата__ДатаЗаписи',
                                                           ulegrul_info).alias('egrul_date_limit').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ОгрДосСв_ГРНДатаИспр__ГРН', ulegrul_info).alias(
                                           'egrul_teh_num_limit').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ОгрДосСв_ГРНДатаИспр__ДатаЗаписи',
                                                           ulegrul_info).alias('egrul_teh_date_limit').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвРегОрг__КодНО', ulegrul_info).alias(
                                           'no_code').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвРегОрг__НаимНО', ulegrul_info).alias(
                                           'no_name').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_ВидЗаяв__КодСЗОЮЛ',
                                                           ulegrul_info).alias('szoul_code').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_ВидЗаяв__НаимСЗОЮЛ',
                                                           ulegrul_info).alias('szoul_name').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвЮЛ__ОГРН', ulegrul_info).alias(
                                           'ogrn').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвЮЛ__ИНН', ulegrul_info).alias(
                                           'inn').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвЮЛ__НаимЮЛПолн', ulegrul_info).alias(
                                           'ulfull_name').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвУпрОрг__ОГРН', ulegrul_info).alias(
                                           'ogrn2').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвУпрОрг__ИНН', ulegrul_info).alias(
                                           'inn2').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвУпрОрг__НаимЮЛПолн',
                                                           ulegrul_info).alias('ulfull2_name').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвФИОИНН__Фамилия',
                                                           ulegrul_info).alias('surname').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвФИОИНН__Имя',
                                                           ulegrul_info).alias('name').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвФИОИНН__Отчество',
                                                           ulegrul_info).alias('patronomyc').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвФИОИНН__ИННФЛ',
                                                           ulegrul_info).alias('innfl').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвФИОИНН_ГРНДатаИспр__ГРН',
                                                           ulegrul_info).alias('egrul_teh_num').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвФИОИНН_ГРНДатаИспр__ДатаЗаписи',
                                                           ulegrul_info).alias('egrul_teh_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвРожд__ДатаРожд',
                                                           ulegrul_info).alias('birth_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвРожд__МестоРожд',
                                                           ulegrul_info).alias('birth_place').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвРожд__ПрДатаРожд',
                                                           ulegrul_info).alias('birth_type').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвРожд_ГРНДатаИспр__ГРН',
                                                           ulegrul_info).alias('egrul2_teh_num').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвЗаявФЛ_СвФЛ_СвРожд_ГРНДатаИспр__ДатаЗаписи',
                                                           ulegrul_info).alias('egrul2_teh_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СведПредДок_НаимДок', ulegrul_info).alias(
                                           'viddoc2_name').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СведПредДок_НомДок', ulegrul_info).alias(
                                           'viddoc2_ser').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СведПредДок_ДатаДок', ulegrul_info).alias(
                                           'viddoc2_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСвид__Серия', ulegrul_info).alias(
                                           'blank_ser').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСвид__Номер', ulegrul_info).alias(
                                           'blank_num').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСвид__ДатаВыдСвид', ulegrul_info).alias(
                                           'blank_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ГРНДатаИспрПред__ИдЗап', ulegrul_info).alias(
                                           'id_info').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ГРНДатаИспрПред__ГРН', ulegrul_info).alias(
                                           'grn').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ГРНДатаИспрПред__ДатаЗап', ulegrul_info).alias(
                                           'info_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ГРНДатаНедПред__ИдЗап', ulegrul_info).alias(
                                           'id2_info').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ГРНДатаНедПред__ГРН', ulegrul_info).alias(
                                           'grn2').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_ГРНДатаНедПред__ДатаЗап', ulegrul_info).alias(
                                           'info2_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСтатусЗап_ГРНДатаНед__ИдЗап',
                                                           ulegrul_info).alias('id3_info').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСтатусЗап_ГРНДатаНед__ГРН',
                                                           ulegrul_info).alias('grn3').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСтатусЗап_ГРНДатаНед__ДатаЗап',
                                                           ulegrul_info).alias('info3_date').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСтатусЗап_ГРНДатаИспр__ИдЗап',
                                                           ulegrul_info).alias('id4_info').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСтатусЗап_ГРНДатаИспр__ГРН',
                                                           ulegrul_info).alias('grn4').cast('string'),
                                       check_column_exists('_СвЗапЕГРЮЛ_СвСтатусЗап_ГРНДатаИспр__ДатаЗап',
                                                           ulegrul_info).alias('info4_date').cast('string')
                                       ).withColumn('table_guid', f.expr("uuid()")).withColumn('ulfull_name', f.regexp_replace(f.col('ulfull_name'), '[\n\r\t]', ''))

    return ulegrul_info


def create_adremail_info(df):
    adremail_info = spark_read(df.select(
        f.col('table_guid'),
        f.col('load_date'),
        f.col('folder_date'),
        check_column_exists('СвАдрЭлПочты.*', df)))

    adremail_info = adremail_info.select(f.col('table_guid').alias('rodtable_guid'),
                f.col('load_date'),
                f.col('folder_date'),
                check_column_exists('_E-mail', adremail_info).alias('email').cast('string'),
                check_column_exists('ГРНИПДата__ГРНИП', adremail_info).alias('egrip_num').cast('string'),
                check_column_exists('ГРНИПДата__ДатаЗаписи', adremail_info).alias('egrip_date').cast('string'),
                check_column_exists('ГРНИПДатаИспр__ГРНИП', adremail_info).alias('egrip_teh_num').cast('string'),
                check_column_exists('ГРНИПДатаИспр__ДатаЗаписи', adremail_info).alias('egrip_teh_date').cast('string')
                ).withColumn('table_guid', f.expr("uuid()"))
    return adremail_info