import re
from tap_logmeinrescue.streams.base import BaseLogMeInRescueReportStream

class TransferredSessionsExtendedReportStream(
    BaseLogMeInRescueReportStream
):
    TABLE = 'transferred_sessions_extended_report'
    KEY_PROPERTIES = []
    API_METHOD = 'GET'
    REPORT_AREA = 16
    REQUIRES = ['technicians']
