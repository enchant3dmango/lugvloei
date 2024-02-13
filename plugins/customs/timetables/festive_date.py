from datetime import timedelta

from pendulum import Date, DateTime, Time
from typing import Optional
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class FestiveDateSchedule(Timetable):
    # Set data interval configuration
    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        if run_after.month == run_after.day or run_after.day == 25:
            delta = timedelta(hours=1)
        else:
            delta = timedelta(minutes=15)
        return DataInterval(start=run_after, end=(run_after + delta))

    # Set next schedule configuration
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:

        if last_automated_data_interval is not None:
            last_start = last_automated_data_interval.start
            if last_start.month == last_start.day or last_start.day == 25:
                delta = timedelta(hours=1)
            else:
                delta = timedelta(minutes=15)
            next_start = last_start + delta
        elif restriction.earliest is None:
            return None
        elif not restriction.catchup:
            next_start = DateTime.combine(Date.today(), Time.min)
        else:
            next_start = restriction.earliest

        if restriction.latest is not None and next_start > restriction.latest:
            return None

        return DagRunInfo.interval(start=next_start, end=(next_start + delta))


class UnevenIntervalsTimetablePlugin(AirflowPlugin):
    name = "uneven_intervals_timetable_plugin"
    timetables = [FestiveDateSchedule]
