class TestSuite:
    row_count_sql = """SELECT COUNT(*) FROM {table}"""

    def row_count_test(records):
        """Check for empty result sets on a table"""
        if len(records) < 1 or len(records[0]) < 1:
            return False

        num_records = records[0][0]
        if num_records < 1:
            return False

        return True
