import datetime
import funcy
import pytz
import singer

from dateutil.parser import parse

from tap_framework.streams import BaseStream
from tap_framework.config import get_config_start_date
from tap_logmeinrescue.state import get_last_record_value_for_table, \
    incorporate, save_state

from tap_logmeinrescue.logger import LOGGER


class BaseLogMeInRescueStream(BaseStream):

    def convert_key(self, k):
        # replace disallowed characters used in keys
        k = k.replace('–', '')
        k = k.replace('/', '_')

        # replace whitespace and downcase everything
        return k.replace(' ', '_').lower()

    def convert_keys(self, d):
        to_return = {}

        for k, v in d.items():
            to_return[self.convert_key(k)] = v

        return to_return

    def transform_record(self, record):
        converted = self.convert_keys(record)

        return super().transform_record(converted)


class BaseLogMeInRescueReportStream(BaseLogMeInRescueStream):

    REPORT_AREA = None

    def get_url(self):
        return 'https://secure.logmeinrescue.com/API/getReport_v2.aspx'

    def generate_catalog(self, custom_field_schema={}):
        schema = self.get_schema(custom_field_schema=custom_field_schema)
        mdata = singer.metadata.new()

        mdata = singer.metadata.write(
            mdata,
            (),
            'inclusion',
            'available'
        )

        for field_name, field_schema in schema.get('properties').items():
            inclusion = 'available'

            if field_name in self.KEY_PROPERTIES:
                inclusion = 'automatic'

            mdata = singer.metadata.write(
                mdata,
                ('properties', field_name),
                'inclusion',
                inclusion
            )

        return [{
            'tap_stream_id': self.TABLE,
            'stream': self.TABLE,
            'key_properties': self.KEY_PROPERTIES,
            'schema': schema,
            'metadata': singer.metadata.to_list(mdata)
        }]

    def get_schema(self, custom_field_schema={}):
        schema = self.load_schema_by_name(self.TABLE)
        schema['properties'] = funcy.merge(
            custom_field_schema,
            schema['properties'])

        return schema

    def header_to_string_schema(self, header):
        to_return = {}

        for key in header:
            new_key = self.convert_key(key)
            to_return[new_key] = {
                "type": ["string", "null"]
            }

        return to_return

    def get_header(self, response):
        status_removed = '\n'.join(response.splitlines()[2:])
        lines = status_removed.split('|\n')
        header_line = lines[0]
        header = header_line.split('|')

        return header

    def get_stream_data(self, response):
        # TODO: Looks like this is dropping the first row if not overriden
        status_removed = '\n'.join(response.splitlines()[2:])
        lines = status_removed.split('|\n')
        rows = lines[1:]
        header = self.get_header(response)

        to_return = []

        for row in rows:
            to_add = {}
            data = row.split('|')

            if len(data) != len(header):
                continue

            for index, item in enumerate(data):
                to_add[header[index]] = item

            to_return.append(self.transform_record(to_add))

        return to_return

    def sync_data(self, parent_ids, return_first_response=False):
        table = self.TABLE

        if not return_first_response:
            self.write_schema()

        start_date = get_last_record_value_for_table(
            self.state, table, 'start_date')
        technician_id = get_last_record_value_for_table(
            self.state, table, 'technician_id')

        if start_date is None:
            start_date = get_config_start_date(self.config)
        else:
            start_date = parse(start_date)

        if technician_id is None:
            technician_id = 0

        end_date = start_date + datetime.timedelta(days=7)

        self.client.make_request(
            'https://secure.logmeinrescue.com/API/setReportArea_v8.aspx',
            'POST',
            params={'area': self.REPORT_AREA})

        while start_date < datetime.datetime.now(tz=pytz.UTC):
            for index, parent_id in enumerate(parent_ids):
                if parent_id < technician_id:
                    continue

                LOGGER.info(
                    ('Fetching session report for technician {} ({}/{}) '
                     'from {} to {}')
                    .format(parent_id, index + 1, len(parent_ids), start_date, end_date))

                self.client.make_request(
                    'https://secure.logmeinrescue.com/API/setReportDate_v2.aspx',  # noqa
                    'POST',
                    params={
                        'bdate': start_date.strftime('%-m/%-d/%Y %-H:%M:%S'),
                        'edate': end_date.strftime('%-m/%-d/%Y %-H:%M:%S')
                    })

                response = self.client.make_request(
                    self.get_url(),
                    'GET',
                    params={
                        'node': parent_id,
                        'nodetype': 'NODE',
                    })

                if return_first_response:
                    return response

                to_write = self.get_stream_data(response)

                if not return_first_response:
                    with singer.metrics.record_counter(endpoint=table) as ctr:
                        singer.write_records(table, to_write)

                        ctr.increment(amount=len(to_write))

                    self.state = incorporate(
                        self.state, table, 'technician_id', parent_id)
                    self.state = incorporate(
                        self.state, table, 'start_date', start_date)
                    self.state = save_state(self.state)

                elif len(to_write) > 0:
                    return to_write[0]

            start_date = end_date
            end_date = start_date + datetime.timedelta(days=7)
            technician_id = 0

            if not return_first_response:
                self.state = incorporate(
                    self.state, table, 'start_date', start_date)
                self.state = incorporate(
                    self.state, table, 'technician_id', technician_id,
                    force=True)
                self.state = save_state(self.state)
