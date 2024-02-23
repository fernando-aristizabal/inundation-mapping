import argparse
import csv
import logging
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing import Manager
from timeit import default_timer as timer

from osgeo import gdal, osr


@dataclass
class HUCProcessingRecord:
    """Holds processing record for each huc."""

    huc: str
    start_time: datetime
    status: str = ""
    error: str = ""
    message: str = ""
    end_time: datetime = field(default_factory=datetime.now)

    def update_on_error(self, error_type: str, error_message: str):
        self.end_time = datetime.now()
        self.error = error_type
        self.status = "failed"
        self.message = error_message

    def update_on_success(self):
        self.end_time = datetime.now()
        self.status = "success"


def get_raster_resolution(gdal_path):
    dataset = gdal.Open(gdal_path, gdal.GA_ReadOnly)
    if not dataset:
        return None, None
    gt = dataset.GetGeoTransform()
    srs = osr.SpatialReference()
    srs.ImportFromWkt(dataset.GetProjection())
    dataset = None
    return max(gt[1], -gt[5]), srs


units_map = {"feet": ["us survey foot"]}


def is_resolution_in_units(srs, units):
    return srs.GetLinearUnitsName().lower() in units_map[units]


def translate_huc(
    huc,
    gdb_gdal_path,
    source_max_resolution,
    source_max_resolution_units,
    output_crs,
    output_resolution,
    output_dir,
    log_level,
    log_folder: str,
    lock,
) -> None:
    start_time = datetime.now()

    logger = setup_logging(log_level, f"{log_folder}/{huc}")
    processing_record = HUCProcessingRecord(huc=huc, start_time=start_time)
    logger.info("Starting processing...")
    try:
        output_hucdir_path = os.path.join(output_dir, huc)
        tmp_dir_path = os.path.join(output_dir, "tmp")

        # 1. Check Raster Resolution and CRS
        res, srs = get_raster_resolution(f"OpenFileGDB:{gdb_gdal_path}:BLE_DEP01PCT")
        if res is None or not is_resolution_in_units(srs, source_max_resolution_units) or res > source_max_resolution:
            logger.error(f"Incorrect resolution or SRS: {res} | {srs}")
            processing_record.update_on_error("IncorrectResolution", "")

        logger.info("Reprojecting floodplain...")
        # 2. Reproject Vector Layer
        reprojected_vector = os.path.join(tmp_dir_path, f"{huc}.shp")
        os.makedirs(os.path.dirname(reprojected_vector), exist_ok=True)
        ogr_cmd = ["ogr2ogr", "-t_srs", output_crs, reprojected_vector, gdb_gdal_path, "FLD_HAZ_AR"]
        subprocess.run(ogr_cmd, check=True)

        risks = [["Moderate", "500yr", ""], ["High", "100yr", ""]]

        # 3. Convert Vector to Raster for each EST_Risk value
        for risk_value in risks:
            logger.info(f"Creating inundation map for {risk_value[1]}...")
            output_raster = os.path.join(output_hucdir_path, risk_value[1], f"ble_huc_{huc}_extent_{risk_value[1]}.tif")
            os.makedirs(os.path.dirname(output_raster), exist_ok=True)
            gdal_rasterize_cmd = [
                "gdal_rasterize",
                "-burn",
                "1",
                "-where",
                f"EST_Risk='{risk_value[0]}'",
                "-at",
                "-tr",
                str(output_resolution),
                str(output_resolution),
                "-ot",
                "Int16",
                "-co",
                "COMPRESS=LZW",
                reprojected_vector,
                output_raster,
            ]
            subprocess.run(gdal_rasterize_cmd, check=True)
            risk_value[2] = output_raster

        gdal_merge_cmd = [
            "gdal_calc.py",
            "-A",
            risks[0][2],
            "-B",
            risks[1][2],
            f"--outfile={risks[0][2]}",
            "--co",
            "COMPRESS=LZW",
            '--calc="A+B>0"',
            "--overwrite",
        ]
        subprocess.run(gdal_merge_cmd, check=True)

        logger.info(f"Completed in {datetime.now() - start_time}")
        processing_record.update_on_success()

    except Exception as e:
        logger.error(f"{huc}: {str(e)}")
        processing_record.update_on_error("UnknownError", str(e))

    finally:
        with lock:
            with open(f"{log_folder}/hucs.csv", "a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [
                        processing_record.huc,
                        processing_record.status,
                        processing_record.error,
                        processing_record.message,
                        processing_record.start_time,
                        processing_record.end_time,
                    ]
                )


def setup_logging(log_level: int, name: str) -> logging.Logger:
    if not isinstance(log_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    # Create a new logger for tihs name
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Configure file handler
    file_handler = logging.FileHandler(f"{name}.log")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(module)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    )

    logger.addHandler(file_handler)
    logger.propagate = False

    return logger


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Create BLE Benchmark Inundation Maps")
    parser.add_argument("-o", "--output_dir", required=True, type=str, help="Directory path for output data.")
    parser.add_argument("-oc", "--output_crs", required=True, type=str, help="")
    parser.add_argument("-or", "--output_resolution", required=True, type=int, help="")
    parser.add_argument("-smr", "--source_max_resolution", required=True, type=int, help="")
    parser.add_argument("-su", "--source_units", required=True, type=str, help="")
    parser.add_argument(
        "-pp", "--parallel_processes_count", default=None, type=int, help="Number of hucs to process simultaneously."
    )
    parser.add_argument(
        "-ll", "--log_level", default="INFO", type=str, help="Set the logging level (e.g., INFO, DEBUG)."
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    run_time = datetime.now()
    run_time_str = run_time.strftime("%Y_%m_%d_%H_%M_%S")

    os.makedirs(run_time_str)
    log_level = getattr(logging, args.log_level.upper(), None)
    logger = setup_logging(log_level, f"{run_time_str}/main")

    # Setup huc processing records CSV
    with open(f"{run_time_str}/hucs.csv", "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["huc", "status", "error", "message", "start_time", "end_time"])

    # Load gdal_paths from CSV
    hucs_list = []
    with open("bfe_hucs_gdal_paths.csv", mode="r") as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader, None)  # Skip the header
        for row in csvreader:
            hucs_list.append(row)

    m = Manager()
    lock = m.Lock()

    logger.info(f"Executing individual hucs...")
    with ProcessPoolExecutor(max_workers=args.parallel_processes_count) as executor:
        for row in hucs_list:
            executor.submit(
                translate_huc,
                row[0],
                row[1],
                args.source_max_resolution,
                args.source_units,
                args.output_crs,
                args.output_resolution,
                args.output_dir,
                log_level,
                run_time_str,
                lock,
            )

    logger.info(f"Completed in {datetime.now() - run_time}")


if __name__ == "__main__":
    main()
