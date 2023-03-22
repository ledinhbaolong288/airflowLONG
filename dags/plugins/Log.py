def Log_process(conn, status, error="ERROR"):
    query = ''
    if status == '0':
        query = """
      UPDATE [RETAILRISK].[dbo].[LOG_ETL_PROCESS]
      SET STATUS = 1, END_PROCESS_DTTM = GETDATE()
      WHERE LOG_ID = 0;
      """
        conn.execute(query)
        conn.commit()
    else:
        query = f"""
      UPDATE [RETAILRISK].[dbo].[LOG_ETL_PROCESS]
      SET STATUS = 2, OBJECT_NOTE = {error}
      WHERE LOG_ID = 0;
      """
        conn.execute(query)
        conn.commit()
