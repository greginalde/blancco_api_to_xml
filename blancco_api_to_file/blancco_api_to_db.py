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
                    print('Failed to write tag')
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

def call_blancco_api(api_url, api_user, api_password, from_date, to_date):
    
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

        response = requests.post(api_url, auth=(api_user, api_password), files=files, verify=False, timeout=180)
        
        if response.status_code == 200:
            log('Successful response from API')
            return response.text
        else:
            if response.text.find('No reports found') > 0:
                return None
            else:
                log('Blancco API Request Status: {}; Message: {}'.format(response.status_code, response.text))
                raise Exception(response.text)

    except Exception as ex:
        raise Exception('Failed API call to Blancco', ex)

def write_data_files(df):

    try:

        # clean data frame, remove any row that only contains duplicates
        df.drop_duplicates(keep='first', inplace=True)

        # drop records that do not contain a document_id
        df = df[pd.notnull(df['description.document_id'])].copy()

        # Truncate data over 4000 chars
        truncate_columns = ['user_data.fields.batterycharging', 'user_data.fields.comments', 'user_data.fields.country', 'user_data.fields.device_identifier', 'user_data.fields.erasure_person', 'user_data.fields.imei_2', 'user_data.fields.imei_3', 'user_data.fields.oppo_device_imeicache_1', 'user_data.fields.oppo_device_imeicache_2', 'user_data.fields.persist_sys_show_device_imei_1', 'user_data.fields.persist_sys_updater_imei_1', 'user_data.fields.persist_sys_updater_imei_2', 'user_data.fields.r_counter', 'user_data.fields.r_country', 'user_data.fields.r_erasure', 'user_data.fields.r_esim', 'user_data.fields.r_fmip', 'user_data.fields.r_frp', 'user_data.fields.r_location', 'user_data.fields.r_mdm', 'user_data.fields.r_place', 'user_data.fields.r_process', 'user_data.fields.r_region', 'user_data.fields.r_workstaion', 'user_data.fields.r_workstation', 'user_data.fields.ro_config_hw_imei_sv_enable_1', 'user_data.fields.ro_config_hw_imei_sv_show_two_2', 'user_data.fields.ro_imei_match_status_3', 'user_data.fields.ro_product_imeisv_3', 'user_data.fields.technician_name']
        for col in list(set(truncate_columns).intersection(list(df.columns.values))):
            if df[col].dtype == 'object':
                df[col] = df[col].str[:4000]

    except Exception as ex:
        raise ex

def log(msg):
    
    print('{}: {}'.format(datetime.utcnow().isoformat(' '), msg))

def get_parms():

    control_file_path = get_control_file_path()
    
    if not os.path.exists(control_file_path):
        raise Exception('Control file not found')
    
    control = json.load(open(control_file_path, 'r'))

    from_date = datetime.strptime(control['to_date'][:19], '%Y-%m-%d %H:%M:%S')
    to_date = from_date + timedelta(hours=1)

    if to_date > datetime.utcnow():
        to_date = from_date

    blancco_url = control['blancco_url']
    blancco_username = control['blancco_username']
    blancco_password = control['blancco_password']
    results_path = control['results_path']

    return {
        'from_date': from_date,
        'to_date': to_date,
        'blancco_url': blancco_url,
        'blancco_username': blancco_username,
        'blancco_password': blancco_password,
        'results_path': results_path
    }


def get_control_file_path():
    script_folder = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(script_folder, 'control.json')


def write_control_file(executeParms):
    with open(get_control_file_path(), 'w') as file:
        json.dump(executeParms, file, indent=4, cls=DateTimeEncoder)

def main():

    try:
        
        executeParms = get_parms()

        if executeParms['to_date'] == executeParms['from_date']:
            log('Nothing to process')

        while executeParms['to_date'] != executeParms['from_date']:
            
            api_url = executeParms['blancco_url']
            api_user = executeParms['blancco_username']
            api_password = executeParms['blancco_password']

            xml = call_blancco_api(api_url, api_user, api_password, executeParms['from_date'], executeParms['to_date'])
            
            if xml:

                xml_data = etree.parse(io.BytesIO(xml.encode('utf-8')))
                xml_data = reformat_xml(xml_data)

                # Parse each report in XML and create a dataframe from the results
                reports = []

                for report in xml_data.xpath('./report'):
                    for record in parse_report(report):
                        reports.append(record)
                
                df = pd.DataFrame(reports)

                # Results file creation
                log('Result files write started')
                write_data_files(df)
                log('Result files write ended')
            
            else:

                log('No reports found')

            write_control_file(executeParms)
            executeParms = get_parms()
    
    except Exception as ex:

        log(ex)
        sys.exit(1)

if __name__ == '__main__':
    main()

