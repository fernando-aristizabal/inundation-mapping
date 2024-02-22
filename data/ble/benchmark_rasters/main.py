import json
import os
import subprocess
from osgeo import gdal, osr

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

# Load config
with open('config.json', 'r') as file:
    config = json.load(file)

with open('gdbs_gdal_paths.json', 'r') as file:
    gdal_paths = json.load(file)

output_crs = config['output_crs']
output_resolution = config['output_resolution']
source_max_resolution = config['source_max_resolution']
source_max_resolution_units = config['source_max_resolution_unit']

for path in gdal_paths:
    huc_basename = os.path.splitext(os.path.basename(path))[0]
    output_base_path = os.path.join("/data/outputs", huc_basename)

    # 1. Check Raster Resolution and CRS
    res, srs = get_raster_resolution(f"OpenFileGDB:{path}:BLE_DEP01PCT")
    if res is None or not is_resolution_in_units(srs, source_max_resolution_units) or res > source_max_resolution:
        continue

    # # 2. Reproject Vector Layer
    reprojected_vector = os.path.join(output_base_path, "reprojected_vector.shp")
    os.makedirs(os.path.dirname(reprojected_vector), exist_ok=True)
    ogr_cmd = ["ogr2ogr", "-t_srs", output_crs, reprojected_vector, path, "FLD_HAZ_AR"]
    subprocess.run(ogr_cmd, check=True)

    risks =[["Moderate", "500yr", ""],["High", "100yr", ""]]

    # 3. Convert Vector to Raster for each EST_Risk value
    for risk_value in risks:
        output_raster = os.path.join(output_base_path, f"ble_huc_{huc_basename}_extent_{risk_value[1]}.tif")
        os.makedirs(os.path.dirname(output_raster), exist_ok=True)
        gdal_rasterize_cmd = ["gdal_rasterize", "-burn", "1", "-where", f"EST_Risk='{risk_value[0]}'", "-at","-tr", str(output_resolution), str(output_resolution), "-ot", "Int16", "-co", "COMPRESS=LZW", reprojected_vector, output_raster]
        subprocess.run(gdal_rasterize_cmd, check=True)
        risk_value[2] = output_raster

    gdal_merge_cmd = ["gdal_calc.py", "-A", risks[0][2], "-B", risks[1][2], f'--outfile={risks[0][2]}', "--co", "COMPRESS=LZW", '--calc="A+B>0"', "--overwrite"]
    subprocess.run(gdal_merge_cmd, check=True)


