import pandas as pd
from functools import wraps

def na_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        rows = func(*args, **kwargs)
        for row in rows:
            tracking_keys = [key for key in row if key.startswith('tracking_number')]
            carrier_keys = [key for key in row if key.startswith('carrier')]
            if any(row.get(k) == 'N/A' for k in tracking_keys + carrier_keys):
                row['order_status'] = 'N/A'
        return rows
    return wrapper

@na_decorator
def prepare_rows(data, carrier_dict):
    rows = []
    max_tracks = 0

    for order_number, details in data.items():
        row = {
            'order_number': order_number,
            'order_status': 'N/A'
        }

        if isinstance(details, list):
            order_status = details[0]
            row['order_status'] = order_status

            if len(details) > 1 and isinstance(details[1], list):
                track_data = details[1]
                for idx, entry in enumerate(track_data, start=1):
                    tn = entry.get('Tracking Number', 'N/A')
                    cr = entry.get('Carrier', 'N/A')
                    row[f'tracking_number{idx}'] = tn
                    row[f'carrier{idx}'] = carrier_dict.get(cr, cr)
                max_tracks = max(max_tracks, len(track_data))
            else:
                row['tracking_number1'] = 'N/A'
                row['carrier1'] = 'N/A'
        else:
            # fallback structure
            row['tracking_number1'] = details[1]
            row['carrier1'] = 'N/A'

        rows.append(row)

    return rows

def dump_to_csv(data, file_path, carrier_dict):
    rows = prepare_rows(data, carrier_dict)

    # Determine dynamic tracking and carrier columns
    dynamic_columns = set()
    for row in rows:
        dynamic_columns.update(k for k in row if k.startswith('tracking_number') or k.startswith('carrier'))

    dynamic_columns = sorted(dynamic_columns, key=lambda x: (x.rstrip('1234567890'), int(''.join(filter(str.isdigit, x)) or 0)))

    all_columns = ['order_number', 'order_status'] + dynamic_columns

    df = pd.DataFrame(rows, columns=all_columns)
    df.to_csv(file_path, index=False)

    print(f"Data successfully written to {file_path}")
