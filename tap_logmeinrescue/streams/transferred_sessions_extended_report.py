import re
from tap_logmeinrescue.streams.base import BaseLogMeInRescueReportStream

LINE_MATCHER = re.compile(r"\|\n(\d+\|(?:\n|.)+?)\|\n", re.MULTILINE)

class TransferredSessionsExtendedReportStream(
    BaseLogMeInRescueReportStream
):
    TABLE = 'transferred_sessions_extended_report'
    KEY_PROPERTIES = ['session_id']
    API_METHOD = 'GET'
    REPORT_AREA = 16
    REQUIRES = ['technicians']

    def get_stream_data(self, response):
        status_removed = re.sub("^OK\n\n", "", response)
        header_line = status_removed[:status_removed.find('|\n')]
        rows = LINE_MATCHER.findall(status_removed)
        header = header_line.split('|')

        to_return = []

        for row in rows:
            if not row:
                continue

            to_add = {}
            data = row.split('|')

            for index, item in enumerate(data):
                # super gross hack: the response is delimited by '|', but
                # the API doesn't escape the delimiter in any way. so,
                # when the chat log has '|' in the text body, it creates
                # extra partial data at the end of the row. just join
                # this onto the already existing record.
                if index >= len(header):
                    to_add[header[len(header) - 1]] += item
                else:
                    to_add[header[index]] = item

            to_return.append(self.transform_record(to_add))

        return to_return
