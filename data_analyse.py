import influxdb_client
import pandas as pd
import numpy as np
from scipy.integrate import trapezoid
from init_db_conn import InitDB
import utilis
import datetime

class AnalyseData(InitDB):
    def __init__(self):
        super().__init__()
        self.error_signal1 = None
        self.time1 = None
        self.error_signal2 = None
        self.time2 = None
        self.start_time = None
        self.data_acquisition_time = None  # Czas ostatniej pobranej próbki

    def fetch_all_data(self, bucket):

        # Query to fetch the data from InfluxDB
        query = utilis.write_combined_query(bucket)
        data = {
            'df_opcua': [],
            'df_opcua2': [],
            'df_opcua3': [],
            'df_opcua4': []
        }

        # Execute the query and process results
        tables = self.query_api.query(query)

        for table in tables:
            for record in table.records:
                time = record.get_time()
                measurement = record.get_measurement()
                field = record.get_field()
                value = record.get_value()

                # Map data to respective DataFrame based on measurement and field
                if measurement == "test-topic" and field == "field3":
                    data['df_opcua'].append({"time": time, "value": value})

                elif measurement == "test-topic2" and field == "field3":
                    data['df_opcua2'].append({"time": time, "value": value})

                elif measurement == "test-topic3" and field == "field3":
                    data['df_opcua3'].append({"time": time, "value": value})

                elif measurement == "test-topic4" and field == "field3":
                    data['df_opcua4'].append({"time": time, "value": value})

        # Debugging: Print the number of records fetched
        print("Number of records df_opcua:", len(data['df_opcua']))
        print("Number of records df_opcua2:", len(data['df_opcua2']))
        print("Number of records df_opcua3:", len(data['df_opcua3']))
        print("Number of records df_opcua4:", len(data['df_opcua4']))

        # Convert data to pandas DataFrames
        df_opcua = pd.DataFrame(data['df_opcua'])
        df_opcua2 = pd.DataFrame(data['df_opcua2'])
        df_opcua3 = pd.DataFrame(data['df_opcua3'])
        df_opcua4 = pd.DataFrame(data['df_opcua4'])

        # Debugging: Log the number of records before synchronization
        print(
            f"Before synchronization: df_opcua={len(df_opcua)}, df_opcua2={len(df_opcua2)}, df_opcua3={len(df_opcua3)}, df_opcua4={len(df_opcua4)}")

        sample_count = 60

        df_opcua = df_opcua.tail(sample_count)
        df_opcua2 = df_opcua2.tail(sample_count)
        df_opcua3 = df_opcua3.tail(sample_count)
        df_opcua4 = df_opcua4.tail(sample_count)

        # Check if there are enough samples for analysis
        if len(df_opcua) < 2 or len(df_opcua2) < 2 or len(df_opcua3) < 2 or len(df_opcua4) < 2:
            raise ValueError("Insufficient number of samples for analysis.")

        # Ensure time columns are in datetime format
        df_opcua['time'] = pd.to_datetime(df_opcua['time'])
        df_opcua2['time'] = pd.to_datetime(df_opcua2['time'])
        df_opcua3['time'] = pd.to_datetime(df_opcua3['time'])
        df_opcua4['time'] = pd.to_datetime(df_opcua4['time'])

        # Ensure value columns are floats
        df_opcua['value'] = df_opcua['value'].astype(float)
        df_opcua2['value'] = df_opcua2['value'].astype(float)
        df_opcua3['value'] = df_opcua3['value'].astype(float)
        df_opcua4['value'] = df_opcua4['value'].astype(float)

        # Synchronize data by merging based on time
        merged_df = pd.merge_asof(
            df_opcua.sort_values('time'),
            df_opcua2.sort_values('time'),
            on='time',
            suffixes=('_opcua', '_opcua2')
        )

        merged_df2 = pd.merge_asof(
            df_opcua3.sort_values('time'),
            df_opcua4.sort_values('time'),
            on='time',
            suffixes=('_opcua3', '_opcua4')
        )

        # Compute time and error signals for analysis
        self.time1 = (merged_df['time'] - merged_df['time'].iloc[0]).dt.total_seconds().values
        self.error_signal1 = merged_df['value_opcua'] - merged_df['value_opcua2']

        self.time2 = (merged_df2['time'] - merged_df2['time'].iloc[0]).dt.total_seconds().values
        self.error_signal2 = merged_df2['value_opcua3'] - merged_df2['value_opcua4']

        # Update the last acquisition time
        self.data_acquisition_time = merged_df['time'].iloc[-1]

        # Debugging: Log the final state
        print(f"Final number of samples: {len(self.time1)} for time1 and {len(self.time2)} for time2")

    def integral_of_error(self, error_signal, time):
        if error_signal is not None and time is not None:
            return trapezoid(error_signal, time)
        return None

    def integral_of_squared_error(self, error_signal, time):
        if error_signal is not None and time is not None:
            return trapezoid(error_signal ** 2, time)
        return None

    def integral_of_absolute_error(self, error_signal, time):
        if error_signal is not None and time is not None:
            return trapezoid(np.abs(error_signal), time)
        return None

    def compute_statistics(self, error_signal):
        return {
            "max": np.max(error_signal),
            "min": np.min(error_signal),
            "mean": np.mean(error_signal),
            "std": np.std(error_signal)
        }

    def analyse(self):
        try:
            self.fetch_all_data(self.bucket)

            err_s1 = self.error_signal1
            err_s2 = self.error_signal2
            time1 = self.time1
            time2 = self.time2

            # Compute integrals for error signal 1
            ie1 = round(self.integral_of_error(err_s1, time1), 4) if self.integral_of_error(err_s1,
                                                                                            time1) is not None else None
            ise1 = round(self.integral_of_squared_error(err_s1, time1), 4) if self.integral_of_squared_error(err_s1,
                                                                                                             time1) is not None else None
            iae1 = round(self.integral_of_absolute_error(err_s1, time1), 4) if self.integral_of_absolute_error(err_s1,
                                                                                                               time1) is not None else None

            # Compute integrals for error signal 2
            ie2 = round(self.integral_of_error(err_s2, time2), 4) if self.integral_of_error(err_s2,
                                                                                            time2) is not None else None
            ise2 = round(self.integral_of_squared_error(err_s2, time2), 4) if self.integral_of_squared_error(err_s2,
                                                                                                             time2) is not None else None
            iae2 = round(self.integral_of_absolute_error(err_s2, time2), 4) if self.integral_of_absolute_error(err_s2,
                                                                                                               time2) is not None else None

            # Compute statistics for error signal 1 and 2
            stats1 = self.compute_statistics(err_s1)
            stats2 = self.compute_statistics(err_s2)

            print(f"Error Signal 1 Stats: {stats1}")
            print(f"Error Signal 2 Stats: {stats2}")

            # Write integrals, statistics, and errors to InfluxDB
            self.write_list_to_influx(ie1, ise1, iae1, ie2, ise2, iae2, self.data_acquisition_time)
            self.write_statistics_to_influx(stats1, stats2, err_s1, err_s2, self.data_acquisition_time)

            print("\nThe results has been sent to InfluxDB...")

            # Display results in console (optional)
            results1 = {
                "IE": ie1,
                "ISE": ise1,
                "IAE": iae1
            }

            results2 = {
                "IE": ie2,
                "ISE": ise2,
                "IAE": iae2
            }

            print("\nResults of the analysis of quality indicators:")
            for key, value in results1.items():
                print(f"{key}: {value:.4f}")

            for key, value in results2.items():
                print(f"{key}: {value:.4f}")

        except ValueError as e:
            print(f"Analysis error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")




