import concurrent.futures
from pathlib import Path

from tqdm import tqdm

from src.gdrive import (
    Credentials,
    GDriveCredential,
    GDriveFileDownloader,
    GDriveReader,
)

DATA_PATH = Path("/home/data/tomocube")
PROJECT_LIST = {
    "2022_tomocube_sepsis": "1Wy-yidV568Hi0ztelaXHC_hv83Wg5DxR",
}


def create_dir(dir_name):
    Path(Path(DATA_PATH, dir_name)).mkdir(parents=True, exist_ok=True)
    return Path(Path(DATA_PATH, dir_name))


def check_file_exist(target_path, image):
    return Path(target_path, image["name"]).exists()


def filter_out_existing_files(image_list):
    return [
        image for image in image_list if not check_file_exist(DATA_PATH, image)
    ]


def download_file(credentials, file_id, target_path, image_name):
    downloader = GDriveFileDownloader(credentials)
    downloader.download(file_id, target_path, image_name)


def download_files(credentials: Credentials, target_path: Path, folder_id: str):
    image_raw_list = GDriveReader(credentials, folder_id).read()
    image_filter_list = filter_out_existing_files(image_raw_list)
    _download_files(image_filter_list, credentials, target_path)


def _download_files(image_filter_list, credentials, target_path):
    if len(image_filter_list) == 0:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [
            executor.submit(
                download_file,
                credentials,
                image["id"],
                target_path,
                image["name"],
            )
            for image in image_filter_list
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()


def main():
    credentials = GDriveCredential().credentials
    patient_list = GDriveReader(
        credentials, PROJECT_LIST["2022_tomocube_sepsis"]
    ).read()

    for patient in tqdm(reversed(patient_list)):
        print(patient)
        target_path = create_dir(patient["name"])
        download_files(credentials, target_path, patient["id"])


if __name__ == "__main__":
    main()
