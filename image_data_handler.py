import requests
from pathlib import Path
from datetime import datetime, timezone
import uuid

def request_frame(server_url, save_dir, filename=None, params={}):
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(
        f"{server_url}",
        params=params,
        timeout=10
    )

    response.raise_for_status()

    image_id = uuid.uuid4().hex
    timestamp = datetime.now().astimezone()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    filename = filename or f"{timestamp_str}_{image_id}.png"
    file_path = save_dir / filename


    with open(file_path,"wb") as f:
        f.write(response.content)

    return{
        "file_name": filename,
        "file_path": file_path,
        "image_id": uuid.uuid4().hex,
        "size_bytes": file_path.stat().st_size,
        "timestamp_utc": timestamp.astimezone(timezone.utc).isoformat(),
    }

if __name__ == "__main__":
    out = request_frame("http://192.168.51.241:5000/lossless_frame", "data/images")
    print(out)