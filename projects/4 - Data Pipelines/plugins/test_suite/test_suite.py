class TestSuite:
    row_count_sql = """SELECT COUNT(*) FROM {table}"""

    @classmethod
    def row_count_test(cls, records):
        """Check for a non-empty result set on a table.
           Raises ValueError if check fails, otherwise returns True.
        """

        if not isinstance(records, list) or not records or not isinstance(records[0], tuple):
            raise ValueError("Invalid records format")

        num_records = records[0][0] if records[0] else 0
        return num_records > 0
