from datetime import timedelta
from typing import Optional

from pendulum import Date, DateTime, Time

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import (DagRunInfo, DataInterval, TimeRestriction,
                                     Timetable)


class OMDateSchedule(Timetable):
    # Set data interval configuration
    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:

        return None

    # Set next schedule configuration
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:

        if last_automated_data_interval is not None:
            last_start = last_automated_data_interval.start
            om_date = [23, 24, 25, 26, 27, 28]

            # H-2, H-1, H, H+1, H+2, H+3 Twin Dates every 09.00, 12.00, 15.00, 18.00, 21.00, 00.00
            if (last_start.month == last_start.day or last_start.month == last_start.day - 1 or
                last_start.month == last_start.day - 2 or last_start.month == last_start.day + 1 or
                    last_start.month == last_start.day + 2 or last_start.month == last_start.day + 3) or \
                    last_start.day in om_date:
                if last_start.hour >= 9 and last_start.hour <= 0:
                    delta = timedelta(hours=3)
        else:
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None

            elif not restriction.catchup:
                next_start = DateTime.combine(
                    Date.today(), Time(9, 0, 0))  # Starts at 9 AM

            if restriction.latest is not None and next_start > restriction.latest:
                return None

        return DagRunInfo.interval(start=next_start, end=(next_start + delta))


class OMDateTimetablePlugin(AirflowPlugin):
    name = "om_date_timetable_plugin"
    timetables = [OMDateSchedule]
