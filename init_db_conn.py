import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import streamlit as st
import datetime
from dotenv import load_dotenv


class InitDB:
    _conn_init = False
    load_dotenv()

    def __init__(self):
        if not InitDB._conn_init:
            self.token = os.getenv("INFLUX_TOKEN")
            self.org = os.getenv("INFLUX_ORG")
            self.url = os.getenv("INFLUX_URL")
            self.bucket = os.getenv("INFLUX_BUCKET")
            self.client = None
            self.write_api = None
            self.query_api = None

            self.connect()
            InitDB._conn_init = True

    def connect(self):
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def write_list_to_influx(self, ie1, ise1, iae1, ie2, ise2, iae2, indices_time):

        points = [
            Point("IE").tag("opcua", "plc").field("IE", ie1).time(indices_time, WritePrecision.NS),
            Point("ISE").tag("opcua", "plc").field("ISE", ise1).time(indices_time, WritePrecision.NS),
            Point("IAE").tag("opcua", "plc").field("IAE", iae1).time(indices_time, WritePrecision.NS),
            Point("IE2").tag("s7conn", "plc").field("IE2", ie2).time(indices_time, WritePrecision.NS),
            Point("ISE2").tag("s7conn", "plc").field("ISE2", ise2).time(indices_time, WritePrecision.NS),
            Point("IAE2").tag("s7conn", "plc").field("IAE2", iae2).time(indices_time, WritePrecision.NS)
        ]

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)

    def write_statistics_to_influx(self, stats1, stats2, err_s1, err_s2, indices_time):

        try:
            # Ensure all stats and error values are primitive types
            stats1 = {key: float(value) for key, value in stats1.items()}
            stats2 = {key: float(value) for key, value in stats2.items()}

            # Correctly access the last value of the error signals
            error1 = float(err_s1.iloc[-1]) if len(err_s1) > 0 else None
            error2 = float(err_s2.iloc[-1]) if len(err_s2) > 0 else None

            # Prepare data points for stats1
            points_stats1 = [
                Point("ErrorStats1").tag("opcua", "plc").field("stat_name", "max").field("value", stats1["max"]).time(
                    indices_time, WritePrecision.NS),
                Point("ErrorStats11").tag("opcua", "plc").field("stat_name", "min").field("value", stats1["min"]).time(
                    indices_time, WritePrecision.NS),
                Point("ErrorStats111").tag("opcua", "plc").field("stat_name", "mean").field("value", stats1["mean"]).time(
                    indices_time, WritePrecision.NS),
                Point("ErrorStats1111").tag("opcua", "plc").field("stat_name", "std").field("value", stats1["std"]).time(
                    indices_time, WritePrecision.NS),
            ]

            # Prepare data points for stats2
            points_stats2 = [
                Point("ErrorStats2").tag("s7conn", "plc").field("stat_name", "max").field("value", stats2["max"]).time(
                    indices_time, WritePrecision.NS),
                Point("ErrorStats22").tag("s7conn", "plc").field("stat_name", "min").field("value", stats2["min"]).time(
                    indices_time, WritePrecision.NS),
                Point("ErrorStats222").tag("s7conn", "plc").field("stat_name", "mean").field("value",
                                                                                           stats2["mean"]).time(
                    indices_time, WritePrecision.NS),
                Point("ErrorStats2222").tag("s7conn", "plc").field("stat_name", "std").field("value", stats2["std"]).time(
                    indices_time, WritePrecision.NS),
            ]

            # Prepare data points for error signals
            points_errors = [
                Point("ErrorValues").tag("opcua", "plc")
                .field("error_signal", "error_signal1").field("value", error1).time(indices_time, WritePrecision.NS),
                Point("ErrorValues").tag("s7conn", "plc")
                .field("error_signal", "error_signal2").field("value", error2).time(indices_time, WritePrecision.NS),
            ]

            # Combine all points
            all_points = points_stats1 + points_stats2 + points_errors

            # Write points to InfluxDB
            self.write_api.write(bucket=self.bucket, org=self.org, record=all_points)
            print("Statystyki i wartości uchybu zostały zapisane do InfluxDB jako osobne punkty.")
        except Exception as e:
            import traceback
            print(f"Failed to write statistics to InfluxDB: {e}")
            print(traceback.format_exc())



