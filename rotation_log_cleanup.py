# run_rotation_log_cleanup.py
def run_rotation_log_cleanup():
    print("🧹 Running cleanup on Rotation_Log...")

    # Auth & Sheet access (as used in other modules)
    # -- omitted here for brevity --

    log_data = log_ws.get_all_values()
    header = log_data[0]
    data = log_data[1:]

    roi_col = header.index("Follow-up ROI") + 1
    for i, row in enumerate(data):
        value = row[roi_col - 1].strip()
        if value and not re.match(r"^-?\d+(\.\d+)?$", value):
            log_ws.update_cell(i + 2, roi_col, "")
            print(f"❌ Non-numeric ROI cleared in row {i+2}: {value}")
