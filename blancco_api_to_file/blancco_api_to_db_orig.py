import argparse
import io
import json
import os
import pandas as pd
import pyodbc
import requests
import urllib3
import sys
from lxml import etree
from collections import OrderedDict
from datetime import date, datetime, timedelta


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, datetime):
            return o.isoformat(sep=' ')

        return json.JSONEncoder.default(self, o)


def flatten(xml, prefix):
    ret = OrderedDict()
    for element in xml:
        if len(element) > 0:
            node_prefix = '{}.{}'.format(prefix, element.tag)
            result = flatten(element, node_prefix)
            ret.update(OrderedDict({key: value for key, value in result.items() if key not in ret}))
        else:
            ret['{}.{}'.format(prefix, element.tag).lower()] = element.text
    return ret


def reformat_xml(xml_data):
    # Find all elements with a name attribute. Update element tag to be the value of the name attribute
    try:
        # log('XML reformat started')
        elements = xml_data.findall('//*[@name]')
        for element in elements:
            # Rename elements only containing the name attribute
            if element.tag in ['entries', 'entry']:
                try:
                    tag_name = str(element.attrib['name']).replace(' ', '_').replace('.', '_')
                    if tag_name[0].isdigit():
                        tag_name = tag_name[2:] + tag_name[0:2][::-1]
                    element.tag = tag_name
                    element.attrib.pop('name')
                except Exception as ex:
                    print('Failed to write tag. {}'.format(ex.message))
                if 'type' in element.attrib:
                    element.attrib.pop('type')
        # log('XML reformat completed')
        return xml_data
    except Exception as ex:
        raise Exception(ex)


def parse_report(xml_data):
    # Get the individual nodes of a report
    description = flatten(xml_data.xpath('./blancco_data/description/*'), 'description')
    hardware = flatten(xml_data.xpath('./blancco_data/blancco_hardware_report/*[not(name()="disks")]'), 'hardware')
    software = flatten(xml_data.xpath('./blancco_data/blancco_software_report/*'), 'software')
    user_data = flatten(xml_data.xpath('./user_data/*'), 'user_data')

    # Store erasures and disks in a list. They will be merge later
    erasures = [flatten(erasure, 'erasure')
                for erasure in xml_data.xpath('./blancco_data/blancco_erasure_report/erasures/*')]
    disks = [flatten(disks, 'disk')
             for disks in xml_data.xpath('./blancco_data/blancco_hardware_report/*[name()="disks"]/*')]

    # Loop over erasures list. This will contain the most iterations per report
    for e in erasures:
        e.update(description)
        e.update(hardware)
        # Join the erasure entry to the correct disk entry
        if len(disks) > 1:
            [e.update(disk) for disk in disks if 'erasure.target.type' in e and disk['disk.type'] == e['erasure.target.type']]
        elif len(disks) == 1:
            e.update(disks[0])
        e.update(software)
        e.update(user_data)
    return erasures


def call_blancco_api(blancco_url, username, password, from_date, to_date):
    try:
        log('API get requested for dates: {} - {}'.format(from_date.isoformat(sep=' '), to_date.isoformat(sep=' ')))
        xml_request = u"""<?xml version="1.0" encoding="UTF-8"?>
                <request>
                    <export-report>
                        <report mode="original"/>
                        <search path="report.report_date" value="{}" operator="gte" datatype="date" conjunction="true" />
                        <search path="report.report_date" value="{}" operator="lt" datatype="date" conjunction="true" />
                    </export-report>
                </request>
                """.format(from_date, to_date)
        files = {'xmlRequest': io.StringIO(xml_request)}
        urllib3.disable_warnings()
        response = requests.post(blancco_url, auth=(username, password), files=files, verify=False, timeout=180)
        if response.status_code == 200:
            log('Successful response from API')
            return response.text
        else:
            if response.text.find('NO REPORTS FOUND') > 0:
                return None
            else:
                log('Blancco API Request Status: {}; Message: {}'.format(response.status_code, response.text))
                raise Exception(response.text)
    except Exception as ex:
        raise Exception('Failed API call to Blancco', ex)


def truncate_stage(cn):
    sql = 'TRUNCATE TABLE [blancco_data_stage]'
    try:
        with pyodbc.connect(cn) as odbc:
            odbc.execute(sql)
    except Exception as ex:
        raise Exception('Failed to truncate staging table. Error: {}'.format(ex), ex)


def write_to_stage(cn, df, batch_size=1000):
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in xrange(0, len(l), n):  # use xrange in python2, range in python3
            yield l[i:i + n]

    sql = ''
    row_count = 0
    try:
        with pyodbc.connect(cn, autocommit=False) as odbc:
            columns_sql = 'SELECT * FROM [blancco_data_stage] WHERE 1=0'
            destination_columns = [column[0].lower() for column in odbc.execute(columns_sql).description]
            mapped_columns = list(set(destination_columns).intersection(list(df.columns.values)))
            unmapped_columns = list(set(df.columns.values) - set(mapped_columns))
            log('Unmapped columns: {}'.format(', '.join(unmapped_columns)))

            cursor = odbc.cursor()
            # cursor.fast_executemany = True
            sql = 'INSERT INTO [blancco_data_stage]({}) VALUES ({})'.format(
                                ', '.join(['[{}]'.format(column) for column in mapped_columns]),
                                ', '.join(['?' for c in mapped_columns]))

            # clean data frame, remove any row that only contains duplicates
            df.drop_duplicates(keep='first', inplace=True)

            # drop records that do not contain a document_id
            df = df[pd.notnull(df['description.document_id'])].copy()

            # Truncate data over 4000 chars
            truncate_columns = ['user_data.fields.batterycharging', 'user_data.fields.comments', 'user_data.fields.country', 'user_data.fields.device_identifier', 'user_data.fields.erasure_person', 'user_data.fields.imei_2', 'user_data.fields.imei_3', 'user_data.fields.oppo_device_imeicache_1', 'user_data.fields.oppo_device_imeicache_2', 'user_data.fields.persist_sys_show_device_imei_1', 'user_data.fields.persist_sys_updater_imei_1', 'user_data.fields.persist_sys_updater_imei_2', 'user_data.fields.r_counter', 'user_data.fields.r_country', 'user_data.fields.r_erasure', 'user_data.fields.r_esim', 'user_data.fields.r_fmip', 'user_data.fields.r_frp', 'user_data.fields.r_location', 'user_data.fields.r_mdm', 'user_data.fields.r_place', 'user_data.fields.r_process', 'user_data.fields.r_region', 'user_data.fields.r_workstaion', 'user_data.fields.r_workstation', 'user_data.fields.ro_config_hw_imei_sv_enable_1', 'user_data.fields.ro_config_hw_imei_sv_show_two_2', 'user_data.fields.ro_imei_match_status_3', 'user_data.fields.ro_product_imeisv_3', 'user_data.fields.technician_name']
            for col in list(set(truncate_columns).intersection(list(df.columns.values))):
                if df[col].dtype == 'object':
                    df[col] = df[col].str[:4000]

            params = []
            for idx, row in df[mapped_columns].iterrows():
                row_count = row_count + 1
                # Check for null values replace with empty string
                params.append([u"{}".format('' if pd.isna(row[column]) else row[column]) for column in mapped_columns])

            try:
                for chunk in chunks(params, batch_size):
                    cursor.executemany(sql, chunk)
                    cursor.commit()
            except Exception as ex:
                raise Exception('Failed to write to database. SQL="{} | Params={}"\nError: {}'.format(sql, chunk, ex), ex)
        log('Loaded {} records to table: [blancco_data_stage]'.format(row_count))
    except Exception as ex:
        raise ex


def update_data_hash(cn):
    sql = """
        UPDATE s
        SET [hash_data] = 
                HASHBYTES('MD5', 
                  CAST([description.document_id] AS VARCHAR(36))
                + ISNULL([erasure.start_time], '')
                + ISNULL([erasure.end_time], '')
                + ISNULL([erasure.erasure_standard_name], '')
                + ISNULL([erasure.timestamp], '')
                + ISNULL([erasure.target.serial], '')
                + ISNULL([hardware.system.imei], '')
                + ISNULL([hardware.system.imei_two], '')
                + ISNULL([hardware.system.meid], '')
                + ISNULL([hardware.system.meid_fourteen], '')
                ) 
        FROM  dbo.blancco_data_stage s    
        """
    try:
        with pyodbc.connect(cn) as odbc:
            odbc.execute(sql)
    except Exception as ex:
        raise Exception('Failed to update checksum. Error: {}'.format(ex), ex)


def write_to_final(cn):
    sql = """
        INSERT INTO [dbo].[blancco_data](
             [business_location]
            ,[business_name]
            ,[erasure_person]
            ,[erasure_provider]
            ,[verified]
            ,[document_id]
            ,[product_name]
            ,[product_revision]
            ,[product_version]
            ,[log_entry_date]
            ,[disk_capacity]
            ,[disk_serial]
            ,[disk_type]
            ,[disk_vendor]
            ,[erasure_elapsed_time]
            ,[erasure_end_time]
            ,[erasure_failure_message]
            ,[erasure_standard_name]
            ,[erasure_exception_message]
            ,[erasure_details_exception_message]            
            ,[erasure_firmware_rounds]
            ,[erasure_overwriting_rounds]
            ,[erasure_start_time]
            ,[erasure_state]
            ,[erasure_target_capacity]
            ,[erasure_target_serial]
            ,[erasure_target_type]
            ,[erasure_target_vendor]
            ,[erasure_timestamp]
            ,[erasure_total_erasure_rounds]
            ,[camera_module_serial]
            ,[camera_serial]
            ,[camera_type]
            ,[battery_capacity_current]
            ,[battery_capacity_design]
            ,[battery_capacity_health_level]
            ,[battery_capacity_wear_level]
            ,[battery_chemical_weighted_ra]
            ,[battery_cycles]
            ,[battery_health_metric]
            ,[battery_manufacture_date]
            ,[battery_serial]
            ,[battery_temperature]
            ,[battery_vendor]
            ,[sim_card]            
            ,[sim_card_iccid]
            ,[sim_card_imsi]
            ,[sim_card_slot]
            ,[sim_card_esim]
            ,[a_model_number]
            ,[carrier_code]            
            ,[chassis_type]
            ,[country_of_origin]
            ,[cover_glass_serial]
            ,[device_color]
            ,[due_diligence_result]
            ,[ecid]
            ,[find_my_iphone]
            ,[find_my_iphone_source]
            ,[frp_status]
            ,[identifier]
            ,[imei]
            ,[imei_two]
            ,[internal_model]
            ,[manufacturer]
            ,[manufacturing_date]
            ,[market_name]
            ,[mdm_status]
            ,[meid]
            ,[meid_fourteen]
            ,[model]
            ,[name]
            ,[product_code]
            ,[project_code]
            ,[ram]
            ,[raw_panel_serial]
            ,[region]
            ,[region_code]
            ,[region_name]
            ,[rooted]
            ,[serial]
            ,[touch_id_serial]
            ,[uuid]
            ,[wifi_mac]
            ,[operating_system]            
            ,[operating_system_name]
            ,[operating_system_program_name]
            ,[operating_system_program_version]
            ,[operating_system_version]
            ,[user_batterycharging]
            ,[user_comments]
            ,[user_country]
            ,[user_device_identifier]
            ,[user_erasure_person]
            ,[user_imei_2]
            ,[user_imei_3]
            ,[user_oppo_device_imeicache_1]
            ,[user_oppo_device_imeicache_2]
            ,[user_persist_sys_show_device_imei_1]
            ,[user_persist_sys_updater_imei_1]
            ,[user_persist_sys_updater_imei_2]
            ,[user_r_counter]
            ,[user_r_country]
            ,[user_r_erasure]
            ,[user_r_esim]
            ,[user_r_fmip]
            ,[user_r_frp]
            ,[user_r_location]
            ,[user_r_mdm]
            ,[user_r_place]
            ,[user_r_process]
            ,[user_r_region]
            ,[user_r_workstaion]
            ,[user_r_workstation]
            ,[user_ro_config_hw_imei_sv_enable_1]
            ,[user_ro_config_hw_imei_sv_show_two_2]
            ,[user_ro_imei_match_status_3]
            ,[user_ro_product_imeisv_3]
            ,[user_technician_name]
            ,[load_datetime]
            ,[hash_data]
            ,[Serial_Number_Dec]
            ,[Serial_Number_Hex]
            ,[Serial_Number_Hex_CD]
            )
        SELECT DISTINCT
             s.[description.description_entries.company_information.business_location]
            ,s.[description.description_entries.company_information.business_name]
            ,s.[description.description_entries.company_information.erasure_person]
            ,s.[description.description_entries.company_information.erasure_provider]
            ,s.[description.description_entries.verified]
            ,s.[description.document_id]
            ,s.[description.document_log.log_entry.author.product_name]
            ,s.[description.document_log.log_entry.author.product_revision]
            ,s.[description.document_log.log_entry.author.product_version]
            ,s.[description.document_log.log_entry.date]
            ,s.[disk.capacity]
            ,s.[disk.serial]
            ,s.[disk.type]
            ,s.[disk.vendor]
            ,s.[erasure.elapsed_time]
            ,s.[erasure.end_time]
            ,s.[erasure.erasure_details.failure.message]
            ,s.[erasure.erasure_standard_name]
            ,s.[erasure.exception.message]
            ,s.[erasure.erasure_details.exception.message]            
            ,s.[erasure.firmware_rounds]
            ,s.[erasure.overwriting_rounds]
            ,s.[erasure.start_time]
            ,s.[erasure.state]
            ,s.[erasure.target.capacity]
            ,s.[erasure.target.serial]
            ,s.[erasure.target.type]
            ,s.[erasure.target.vendor]
            ,s.[erasure.timestamp]
            ,s.[erasure.total_erasure_rounds]
            ,s.[hardware.cameras.camera.module_serial]
            ,s.[hardware.cameras.camera.serial]
            ,s.[hardware.cameras.camera.type]
            ,s.[hardware.mobile_battery.battery_capacity_current]
            ,s.[hardware.mobile_battery.battery_capacity_design]
            ,s.[hardware.mobile_battery.battery_capacity_health_level]
            ,s.[hardware.mobile_battery.battery_capacity_wear_level]
            ,s.[hardware.mobile_battery.battery_chemical_weighted_ra]
            ,s.[hardware.mobile_battery.battery_cycles]
            ,s.[hardware.mobile_battery.battery_health_metric]
            ,s.[hardware.mobile_battery.battery_manufacture_date]
            ,s.[hardware.mobile_battery.battery_serial]
            ,s.[hardware.mobile_battery.battery_temperature]
            ,s.[hardware.mobile_battery.battery_vendor]
            ,s.[hardware.sim_cards.sim_card]
            ,s.[hardware.sim_cards.sim_card.iccid]
            ,s.[hardware.sim_cards.sim_card.imsi]
            ,s.[hardware.sim_cards.sim_card.slot]
            ,s.[hardware.sim_cards.sim_card.esim]
            ,s.[hardware.system.a_model_number]
            ,s.[hardware.system.carrier_code]
            ,s.[hardware.system.chassis_type]
            ,s.[hardware.system.country_of_origin]
            ,s.[hardware.system.cover_glass_serial]
            ,s.[hardware.system.device_color]
            ,s.[hardware.system.due_diligence_result]
            ,s.[hardware.system.ecid]
            ,s.[hardware.system.find_my_iphone]
            ,s.[hardware.system.find_my_iphone_source]
            ,s.[hardware.system.frp_status]
            ,s.[hardware.system.identifier]
            ,s.[hardware.system.imei]
            ,s.[hardware.system.imei_two]
            ,s.[hardware.system.internal_model]
            ,s.[hardware.system.manufacturer]
            ,s.[hardware.system.manufacturing_date]
            ,s.[hardware.system.market_name]
            ,s.[hardware.system.mdm_status]
            ,s.[hardware.system.meid]
            ,s.[hardware.system.meid_fourteen]
            ,s.[hardware.system.model]
            ,s.[hardware.system.name]
            ,s.[hardware.system.product_code]
            ,s.[hardware.system.project_code]
            ,s.[hardware.system.ram]
            ,s.[hardware.system.raw_panel_serial]
            ,s.[hardware.system.region]
            ,s.[hardware.system.region_code]
            ,s.[hardware.system.region_name]
            ,s.[hardware.system.rooted]
            ,s.[hardware.system.serial]
            ,s.[hardware.system.touch_id_serial]
            ,s.[hardware.system.uuid]
            ,s.[hardware.system.wifi_mac]
            ,s.[software.operating_system]
            ,s.[software.operating_system.name]
            ,s.[software.operating_system.programs.program.name]
            ,s.[software.operating_system.programs.program.version]
            ,s.[software.operating_system.version]
            ,s.[user_data.fields.batterycharging]
            ,s.[user_data.fields.comments]
            ,s.[user_data.fields.country]
            ,s.[user_data.fields.device_identifier]
            ,s.[user_data.fields.erasure_person]
            ,s.[user_data.fields.imei_2]
            ,s.[user_data.fields.imei_3]
            ,s.[user_data.fields.oppo_device_imeicache_1]
            ,s.[user_data.fields.oppo_device_imeicache_2]
            ,s.[user_data.fields.persist_sys_show_device_imei_1]
            ,s.[user_data.fields.persist_sys_updater_imei_1]
            ,s.[user_data.fields.persist_sys_updater_imei_2]
            ,s.[user_data.fields.r_counter]
            ,s.[user_data.fields.r_country]
            ,s.[user_data.fields.r_erasure]
            ,s.[user_data.fields.r_esim]
            ,s.[user_data.fields.r_fmip]
            ,s.[user_data.fields.r_frp]
            ,s.[user_data.fields.r_location]
            ,s.[user_data.fields.r_mdm]
            ,s.[user_data.fields.r_place]
            ,s.[user_data.fields.r_process]
            ,s.[user_data.fields.r_region]
            ,s.[user_data.fields.r_workstaion]
            ,s.[user_data.fields.r_workstation]
            ,s.[user_data.fields.ro_config_hw_imei_sv_enable_1]
            ,s.[user_data.fields.ro_config_hw_imei_sv_show_two_2]
            ,s.[user_data.fields.ro_imei_match_status_3]
            ,s.[user_data.fields.ro_product_imeisv_3]
            ,s.[user_data.fields.technician_name]
            ,s.[load_datetime]
            ,s.[hash_data]
            ,CASE
                WHEN LEN(s.[hardware.system.imei]) = 0 THEN ''
                WHEN LEFT(s.[hardware.system.imei], 2) = '35' THEN [SCM_DataAnalytics].[esn].[fn_ESNConversion](LEFT(s.[hardware.system.imei], 14), 'DEC') 
                WHEN LEFT(s.[hardware.system.imei], 2) = '99' THEN [SCM_DataAnalytics].[esn].[fn_ESNConversion](LEFT(s.[hardware.system.imei], 14), 'DEC') 
                ELSE [SCM_DataAnalytics].[esn].[fn_ESNConversion](s.[hardware.system.imei], 'DEC') END AS [Serial_Number_Dec]
            ,CASE
                WHEN LEN(s.[hardware.system.imei]) = 0 THEN ''
                WHEN LEFT(s.[hardware.system.imei], 2) = '35' THEN [SCM_DataAnalytics].[esn].[fn_ESNConversion](LEFT(s.[hardware.system.imei], 14), 'HEX') 
                WHEN LEFT(s.[hardware.system.imei], 2) = '99' THEN [SCM_DataAnalytics].[esn].[fn_ESNConversion](LEFT(s.[hardware.system.imei], 14), 'HEX') 
                ELSE [SCM_DataAnalytics].[esn].[fn_ESNConversion](s.[hardware.system.imei], 'HEX') END AS [Serial_Number_Hex]
            ,CASE
                WHEN LEN(s.[hardware.system.imei]) = 0 THEN ''
                WHEN LEFT(s.[hardware.system.imei], 2) = '35' THEN [SCM_DataAnalytics].[esn].[fnGetLuhn] ([SCM_DataAnalytics].[esn].[fn_ESNConversion](LEFT(s.[hardware.system.imei], 14), 'HEX')) 
                WHEN LEFT(s.[hardware.system.imei], 2) = '99' THEN [SCM_DataAnalytics].[esn].[fnGetLuhn] ([SCM_DataAnalytics].[esn].[fn_ESNConversion](LEFT(s.[hardware.system.imei], 14), 'HEX')) 
                ELSE [SCM_DataAnalytics].[esn].[fnGetLuhn] ([SCM_DataAnalytics].[esn].[fn_ESNConversion](s.[hardware.system.imei], 'HEX')) END AS [Serial_Number_Hex_CD]
        FROM [dbo].[blancco_data_stage] s
        LEFT JOIN [dbo].[blancco_data] d ON d.[hash_data] = s.[hash_data]
        WHERE d.RecId IS NULL 
        """
    try:
        with pyodbc.connect(cn) as odbc:
            rowcount = odbc.execute(sql).rowcount
            log('Loaded {} records to table: [blancco_data]'.format(rowcount))
    except Exception as ex:
        raise Exception('Failed to load final table. Error: {}'.format(ex), ex)


def log(msg):
    print('{}: {}'.format(datetime.utcnow().isoformat(' '), msg))


def get_dates():
    control_file_path = get_control_file_path()
    if not os.path.exists(control_file_path):
        raise Exception('Control file not found')
    control = json.load(open(control_file_path, 'r'))

    from_date = datetime.strptime(control['to_date'][:19], '%Y-%m-%d %H:%M:%S')
    to_date = from_date + timedelta(hours=1)
    if to_date > datetime.utcnow():
        to_date = from_date
    return {
        'from_date': from_date,
        'to_date': to_date
    }


def get_control_file_path():
    script_folder = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(script_folder, 'control.json')


def write_control_file(dates):
    with open(get_control_file_path(), 'w') as file:
        json.dump(dates, file, indent=4, cls=DateTimeEncoder)


def read_args():
    parser = argparse.ArgumentParser(
        description='Connects to the Blancco API and writes the results to a database table'
    )
    parser.add_argument('-cn', '--connection_string',
                        help='Destination ODBC connection string', type=str, required=True)
    parser.add_argument('-url', type=str, required=True,
                        help='This is the Blancco endpoint to call for exporting report data in XML format')
    parser.add_argument('--batch_size', type=int, default=1000,
                        help='The batch size for records inserted into the stage table')
    parser.add_argument('-u', '--username', type=str, required=True,
                        help='This is the username to access the Blancco API')
    parser.add_argument('-p', '--password', type=str,
                        help='This is the password to access the Blancco API. If not supplied a password can be passed via the "blancco_api_pw" environment variable')
    return parser.parse_args()


def main():
    try:
        args = read_args()
        cn_string = args.connection_string
        dates = get_dates()
        if dates['to_date'] == dates['from_date']:
            log('Nothing to process')
        while dates['to_date'] != dates['from_date']:
            password = args.password if args.password else os.environ['blancco_api_pw']
            xml = call_blancco_api(args.url, args.username, password, dates['from_date'], dates['to_date'])
            if xml:
                xml_data = etree.parse(io.BytesIO(xml.encode('utf-8')))
                xml_data = reformat_xml(xml_data)

                # Parse each report in XML and create a dataframe from the results
                reports = []
                # log('Report parsing into dataframe started')
                for report in xml_data.xpath('./report'):
                    for record in parse_report(report):
                        reports.append(record)
                df = pd.DataFrame(reports)
                # log('Report parsing into dataframe complete')

                # Data pipeline
                log('Data pipeline started')
                truncate_stage(cn_string)
                write_to_stage(cn_string, df, batch_size=args.batch_size)
                update_data_hash(cn_string)
                write_to_final(cn_string)
                log('Data pipeline complete')
            else:
                log('No reports found')

            write_control_file(dates)
            dates = get_dates()
    except Exception as ex:
        log(ex)
        sys.exit(1)


if __name__ == '__main__':
    main()

