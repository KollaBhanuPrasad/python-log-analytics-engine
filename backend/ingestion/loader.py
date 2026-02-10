import dask.bag as db

def load_logs(file_path):
    bag = db.read_text(file_path)
    return bag