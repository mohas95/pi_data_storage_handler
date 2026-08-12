import requests
from pathlib import Path
from datetime import datetime, timezone
import uuid
import boto3
import cv2
import shutil


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
        "image_id": image_id,
        "size_bytes": file_path.stat().st_size,
        "timestamp_utc": timestamp.astimezone(timezone.utc).isoformat(),
    }


class ImageDatasetHandler:

    IMAGE_EXTENSIONS = {
        ".jpg", ".jpeg", ".png",
        ".bmp", ".tif", ".tiff"
    }

    def __init__(self, image_db_root_path, aws_s3_creds = None):
        self.image_db_root_path = Path(image_db_root_path)

        self.s3_client = None

        if aws_s3_creds is not None:
            self.s3_client = boto3.client("s3",
                                        aws_access_key_id = aws_s3_creds.get("id"),
                                        aws_secret_access_key= aws_s3_creds.get("access_key"),
                                        region_name=aws_s3_creds.get("region")
                                        )

    def is_image_file(self, image_path):
        image_path = Path(image_path)

        if not image_path.is_file():
            return False

        return Path(image_path).suffix.lower() in self.IMAGE_EXTENSIONS

    def image_valid(self, image_path):
        image_path = Path(image_path)

        try:
            if not self.is_image_file(image_path):
                return False

            if image_path.stat().st_size == 0:
                return False

            image = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)

            return image is not None and image.size > 0

        except (OSError, cv2.error):
            return False

        
    def check_image_group_continuity(self):
        pass

    def get_image_groups(self):
        if not self.image_db_root_path.exists():
            return []

        return [path.name for path in self.image_db_root_path.iterdir() if path.is_dir()]

    def init_s3_client(self, s3_client):
        self.s3_client = s3_client

    def upload_image_to_s3(self, image_path, bucket, group):
        if not self.s3_client:
            print("No AWS S3 client set, please initialize")
            return False

        image_path = Path(image_path)

        s3_key = f"{group}/images/{image_path.name}"

        try:
            self.s3_client.upload_file(
                str(image_path),
                bucket,
                s3_key
            )

            return True

        except Exception as e:
            print(f"Failed to upload {image_path}: {e}")
            return False

    
    def sync_with_s3_bucket(self, group, bucket, limit=None):
        group_path = self.image_db_root_path / group
        synced_path = group_path / "uploaded"

        resp = {
            "attempted_uploads":0,
            "successful_uploads": 0,
            "failed_uploads": 0,
            "deleted_bad_images": 0,
            "failed_moves":0,
            "deleted_images":[]
        }

        if not group_path.exists() or not group_path.is_dir():
            raise ValueError(f"Image group does not exist: {group_path}")

        synced_path.mkdir(parents=True, exist_ok=True)

        for image_path in group_path.iterdir():

            if not self.is_image_file(image_path):
                continue

            if not self.image_valid(image_path):
                if self.delete_image(image_path):
                    resp["deleted_bad_images"] += 1
                    resp["deleted_images"].append(str(image_path.name))

                continue

            if limit is not None and resp["attempted_uploads"] >= limit:
                break

            resp["attempted_uploads"] += 1

            success = self.upload_image_to_s3(image_path, bucket, group)

            if success:
                destination = synced_path / image_path.name
                try:
                    shutil.move(str(image_path), str(destination))
                    resp["successful_uploads"] += 1

                except OSError as e:
                    print(f"Image uploaded but failed to move {image_path}: {e}")
                    resp["failed_moves"] += 1
            else:
                resp["failed_uploads"] += 1

        return resp


    def get_synced_info(self, group):
        group_path = self.image_db_root_path / group
        synced_path = group_path / "uploaded"

        resp = {
            "not_uploaded":[],
            "uploaded": []
        }

        if not group_path.exists() or not group_path.is_dir():
            raise ValueError(f"Image group does not exist: {group_path}")

        synced_path.mkdir(parents=True, exist_ok=True)

        for image_path in group_path.iterdir():

            if self.is_image_file(image_path):
                resp["not_uploaded"].append(image_path.name)

        for image_path in synced_path.iterdir():
            if self.is_image_file(image_path):
                resp["uploaded"].append(image_path.name)

        return resp


    def delete_image(self, image_path):
        image_path = Path(image_path)

        try:
            image_path.unlink(missing_ok=True)
            return True

        except OSError as e:
            print(f"Failed to delete {image_path}: {e}")
            return False

    
    def clear_synced_images(self, group):
        group_path = self.image_db_root_path / group
        synced_path = group_path / "uploaded"

        deleted_files = []

        if not group_path.exists() or not group_path.is_dir():
            raise ValueError(f"Image group does not exist: {group_path}")

        synced_path.mkdir(parents=True, exist_ok=True)

        for image_path in synced_path.iterdir():
            if self.is_image_file(image_path):
                if self.delete_image(image_path):
                    deleted_files.append(image_path.name)

        return deleted_files




if __name__ == "__main__":
    out = request_frame("http://192.168.51.241:5000/lossless_frame", "data/images")
    print(out)