import smartsheet
from datetime import datetime, timedelta
import os
import dotenv
dotenv.load_dotenv()
token=os.getenv("TOKEN")
sheetId=os.getenv("SHEET_ID")

# Create a Smartsheet client.
ss_client = smartsheet.Smartsheet(token)

# Mapping of isoweekday() to day names.
# Note: datetime.isoweekday() returns 7 for Sunday, 1 for Monday, etc.
DAY_OF_WEEK = {
    1: 'MONDAY',
    2: 'TUESDAY',
    3: 'WEDNESDAY',
    4: 'THURSDAY',
    5: 'FRIDAY',
    6: 'SATURDAY',
    7: 'SUNDAY'
}

def format_date(dt):
    """Format a datetime as YYYY-MM-DD."""
    return dt.strftime("%Y-%m-%d")

def is_working_day(dt, working_days, non_working_days):
    """Return True if dt is a working day and not in non-working days."""
    day_str = DAY_OF_WEEK[dt.isoweekday()]
    return day_str in working_days and format_date(dt) not in non_working_days

def get_cell_value(row, column_id):
    """Retrieve the cell value from a row given a column ID."""
    for cell in row.get('cells', []):
        if cell.get('columnId') == column_id:
            return cell.get('value')
    return None

def days_between(predecessor, successor, lag, dependency_type,
                 working_days, non_working_days, start_date_col, end_date_col):
    """
    Calculate the float (in working days) between predecessor and successor dates.
    The dependency_type determines which dates are used.
    """
    # Choose the correct date columns based on dependency type.
    if dependency_type == 'FF':
        p_column, s_column = end_date_col, end_date_col
    elif dependency_type == 'SF':
        p_column, s_column = start_date_col, end_date_col
    elif dependency_type == 'SS':
        p_column, s_column = start_date_col, start_date_col
    else:  # Default to FS.
        p_column, s_column = end_date_col, start_date_col

    # Get the predecessor date.
    p_value = get_cell_value(predecessor, p_column)
    try:
        p_date = datetime.fromisoformat(p_value) if p_value else None
    except Exception:
        p_date = None

    if not p_date:
        return None

    # Get the successor date.
    if isinstance(successor, datetime):
        s_date = successor
    else:
        s_value = get_cell_value(successor, s_column)
        try:
            s_date = datetime.fromisoformat(s_value) if s_value else None
        except Exception:
            s_date = None

    if not s_date:
        return None

    # Adjust the successor date backward by the lag (counting only working days).
    remaining_lag = lag
    days_count = 0
    temp_date = s_date
    while remaining_lag > 0:
        temp_date = temp_date - timedelta(days=1)
        days_count += 1
        if is_working_day(temp_date, working_days, non_working_days):
            remaining_lag -= 1
    adjusted_successor_date = s_date - timedelta(days=days_count)

    # Calculate float: count working days between p_date and the adjusted successor date.
    float_value = 0
    compare_date = adjusted_successor_date
    while p_date < compare_date:
        compare_date = compare_date - timedelta(days=1)
        if is_working_day(compare_date, working_days, non_working_days):
            float_value += 1

    return float_value

def main():
    try:
        # Retrieve the sheet data (including objectValue for cells).
        sheet_response = ss_client.Sheets.get_sheet(
            sheetId,
            include=['objectValue']
        )
        sheet = sheet_response.to_dict()

        working_days = sheet['projectSettings']['workingDays']
        non_working_days = sheet['projectSettings']['nonWorkingDays']

        predecessor_col = None
        start_date_col = None
        end_date_col = None
        float_col = None

        # Identify relevant column IDs.
        for column in sheet.get('columns', []):
            if column.get('type') == 'PREDECESSOR':
                predecessor_col = column.get('id')
            if column.get('tags') and 'GANTT_START_DATE' in column.get('tags'):
                start_date_col = column.get('id')
            if column.get('tags') and 'GANTT_END_DATE' in column.get('tags'):
                end_date_col = column.get('id')
            if column.get('title') == 'Float':
                float_col = column.get('id')

        if not all([predecessor_col, start_date_col, end_date_col, float_col]):
            raise ValueError("One or more required column IDs are missing.")

        rows_by_number = {}
        rows_in_cp = {}             # rowNumber -> dependency dict (in critical path)
        connected_to_cp = {}        # rowNumber -> dependency dict (not in CP)
        not_connected_to_cp = {}    # rowNumber -> row dict
        critical_path_rows = set()
        last_task_end_date = None

        # Process each row.
        for row in sheet.get('rows', []):
            row_number = row.get('rowNumber')
            rows_by_number[row_number] = row

            # Skip Summary Roll Up rows.
            is_summary = any(
                cell.get('columnId') == start_date_col and cell.get('formula') == '=MIN(CHILDREN())'
                for cell in row.get('cells', [])
            )
            if is_summary:
                continue

            for cell in row.get('cells', []):
                if cell.get('columnId') == predecessor_col and cell.get('objectValue'):
                    predecessors = cell.get('objectValue').get('predecessors', [])
                    for dependency in predecessors:
                        if dependency.get('inCriticalPath'):
                            rows_in_cp[row_number] = dependency
                        else:
                            connected_to_cp[row_number] = dependency
                        # Track both the dependency's row and the current row as part of the CP.
                        critical_path_rows.add(dependency.get('rowNumber'))
                        critical_path_rows.add(row_number)
                elif cell.get('columnId') == end_date_col and cell.get('value'):
                    try:
                        cell_date = datetime.fromisoformat(cell.get('value'))
                    except Exception:
                        continue
                    if (last_task_end_date is None) or (cell_date > last_task_end_date):
                        last_task_end_date = cell_date
                elif cell.get('columnId') == predecessor_col:
                    # Mark rows with a predecessor cell but no objectValue.
                    not_connected_to_cp[row_number] = row

        # Build a list of row updates.
        update_rows = []

        # Update Critical Path rows: set float to 0.
        for row_number, dependency in rows_in_cp.items():
            dep_row_number = dependency.get('rowNumber')
            # If the predecessor row is not already updated, update it with float=0.
            if dep_row_number not in rows_in_cp and dep_row_number in rows_by_number:
                update_rows.append({
                    'id': rows_by_number[dep_row_number]['id'],
                    'cells': [{'columnId': float_col, 'value': 0}]
                })
            # Update the current row with float=0.
            if row_number in rows_by_number:
                update_rows.append({
                    'id': rows_by_number[row_number]['id'],
                    'cells': [{'columnId': float_col, 'value': 0}]
                })

        # Update rows connected to the Critical Path.
        for row_number, dependency in connected_to_cp.items():
            predecessor_row = rows_by_number.get(dependency.get('rowNumber'))
            successor_row = rows_by_number.get(int(row_number))
            if not predecessor_row or not successor_row:
                continue
            lag_days = dependency.get('lag', {}).get('days', 0)
            dependency_type = dependency.get('type', 'FS')
            float_value = days_between(
                predecessor_row,
                successor_row,
                lag_days,
                dependency_type,
                working_days,
                non_working_days,
                start_date_col,
                end_date_col
            )
            update_rows.append({
                'id': predecessor_row['id'],
                'cells': [{'columnId': float_col, 'value': float_value}]
            })

        # Update rows not connected to the Critical Path.
        for row_number, row in not_connected_to_cp.items():
            if int(row_number) not in critical_path_rows:
                # Use the last task end date as the successor.
                lag_days = 0
                dependency_type = 'FS'
                float_value = days_between(
                    row,
                    last_task_end_date,
                    lag_days,
                    dependency_type,
                    working_days,
                    non_working_days,
                    start_date_col,
                    end_date_col
                )
                # If float is falsy (e.g. 0 or None), set it to -1.
                if not float_value:
                    float_value = -1
                update_rows.append({
                    'id': row['id'],
                    'cells': [{'columnId': float_col, 'value': float_value}]
                })

        # Submit the row updates.
        response = ss_client.Sheets.update_rows(sheetId, update_rows)
        print("Update result:", response)
        return response

    except Exception as e:
        print("Error processing sheet:", e)
        raise

if __name__ == '__main__':
    main()
