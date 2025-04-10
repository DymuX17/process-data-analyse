
def write_combined_query(bucket):
    query = f"""
    from(bucket: "{bucket}")
    |> range(start: -65s)
    |> filter(fn: (r) => 
        (r._measurement == "test-topic" and r._field == "field3") or 
        (r._measurement == "test-topic2" and r._field == "field3") or
        (r._measurement == "test-topic3" and r._field == "field3") or 
        (r._measurement == "test-topic4" and r._field == "field3")
    )
    """
    return query
