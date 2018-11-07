from tap_logmeinrescue.streams.base import BaseLogMeInRescueStream

import singer

from tap_logmeinrescue.logger import LOGGER


class TechniciansStream(BaseLogMeInRescueStream):
    TABLE = 'technicians'
    KEY_PROPERTIES = ['nodeid']
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://secure.logmeinrescue.com/API/getHierarchy_v2.aspx'

    def get_stream_data(self, response):
        to_return = []
        nodes = response.split('\n\n')[1:-1]

        for node in nodes:
            item = {}
            fields = node.split('\n')

            for field in fields:
                (k, v) = field.split(':')
                item[k] = v

            output = self.transform_record(item)

            if output.get('type') == 'Technician':
                to_return.append(output)

        return to_return

    def sync_data(self, return_ids=False):
        table = self.TABLE

        response = self.client.make_request(self.get_url(), 'GET')

        # In development, the fastest way to decrease iteration time is to
        # slice the following data down to something very small like 10.
        #
        # all_technicians = self.get_stream_data(response)[:10]
        all_technicians = self.get_stream_data(response)

        if not return_ids:
            with singer.metrics.record_counter(endpoint=table) as counter:
                for obj in all_technicians:
                    singer.write_records(
                        table,
                        [obj])

                    counter.increment()

        technician_ids = sorted(
            [technician.get('nodeid')
             for technician in all_technicians])

        if return_ids:
            return technician_ids

        for substream in self.substreams:
            substream.state = self.state
            LOGGER.info("Syncing {}".format(substream.TABLE))
            substream.sync_data(
                parent_ids=technician_ids)
            self.state = substream.state
