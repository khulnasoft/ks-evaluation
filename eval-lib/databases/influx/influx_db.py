from influxdb import InfluxDBClient
from typing import Optional, Dict


class InfluxDBWrapper:
    """
    A wrapper class for interacting with InfluxDB to query process statistics.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8086,
        user: str = "root",
        password: str = "",
        database: str = "",
    ):
        """
        Initializes the InfluxDB client.
        
        Args:
            host (str): InfluxDB host address.
            port (int): InfluxDB port number.
            user (str): Username for authentication.
            password (str): Password for authentication.
            database (str): Database name to connect to.
        """
        self.client = InfluxDBClient(host, port, user, password, database)

    @staticmethod
    def _convert_to_nanoseconds(timestamp: int) -> int:
        """
        Converts a timestamp from seconds to nanoseconds.

        Args:
            timestamp (int): Time in seconds.

        Returns:
            int: Time in nanoseconds.
        """
        return timestamp * 1_000_000_000

    def get_procstat_result(
        self, process_name: str, start_time: int, end_time: int
    ) -> Optional[Dict[str, float]]:
        """
        Retrieves the 90th percentile CPU and memory usage of a specific process over a given time range.

        Args:
            process_name (str): Name of the process.
            start_time (int): Start time in seconds.
            end_time (int): End time in seconds.

        Returns:
            Optional[Dict[str, float]]: A dictionary with `max_cpu_usage` and `max_mem_usage`,
            or `None` if no data is available.

        Example Output:
            {'max_cpu_usage': 10.0, 'max_mem_usage': 10.0}
        """
        try:
            # Convert time units from seconds to nanoseconds
            start_time_ns = self._convert_to_nanoseconds(start_time)
            end_time_ns = self._convert_to_nanoseconds(end_time)

            # Query filter
            filter_condition = f"pattern = '{process_name}'"

            # SQL Query for 90th percentile CPU & memory usage
            sql_query = f"""
                SELECT 
                    percentile(sum_cpu_usage, 90) AS max_cpu_usage, 
                    percentile(sum_memory_usage, 90) AS max_mem_usage
                FROM (
                    SELECT 
                        SUM(cpu_usage) AS sum_cpu_usage, 
                        SUM(memory_usage) AS sum_memory_usage 
                    FROM procstat 
                    WHERE {filter_condition} 
                    AND time >= {start_time_ns} 
                    AND time <= {end_time_ns} 
                    GROUP BY time(10s)
                )
            """

            # Execute query
            result = self.client.query(sql_query)
            procstat = list(result.get_points())

            return procstat[0] if procstat else None

        except Exception as e:
            print(f"[ERROR] Failed to query InfluxDB: {e}")
            return None
