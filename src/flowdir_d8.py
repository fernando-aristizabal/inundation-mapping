#!/usr/bin/env python3

import argparse
import os

import rasterio as rio
import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def flowdir_d8(dem_filename: str, flowdir_filename: str):
    """
    Create flow direction raster from DEM. Wrapper for WhiteboxTools d8_pointer.

    Parameters
    ----------
    dem_filename: str
        DEM filename
    flowdir_filename: str
        Flow accumulation filename
    """

    assert os.path.isfile(dem_filename), 'ERROR: flowdir file not found: ' + str(dem_filename)

    wbt_flowdir_filename = os.path.join(
        os.path.dirname(flowdir_filename), 'wbt-' + os.path.basename(flowdir_filename)
    )

    # Compute WhiteboxTools flow direction
    if wbt.d8_pointer(dem_filename, wbt_flowdir_filename, esri_pntr=False) != 0:
        raise Exception('ERROR: WhiteboxTools d8_pointer failed')

    assert os.path.isfile(wbt_flowdir_filename), 'ERROR: flowdir file not found: ' + str(wbt_flowdir_filename)

    # Reclassify WhiteboxTools flow direction to TauDEM flow direction
    with rio.open(wbt_flowdir_filename) as src:
        profile = src.profile
        nodata = src.nodata
        crs = src.crs

        dem = src.read(1)

    data = dem.copy()

    data[dem == 0] = nodata
    data[dem == 1] = 2
    data[dem == 2] = 1
    data[dem == 4] = 8
    data[dem == 8] = 7
    data[dem == 16] = 6
    data[dem == 32] = 5
    data[dem == 64] = 4
    data[dem == 128] = 3

    del dem

    # Write output
    with rio.open(flowdir_filename, "w", **profile) as dst:
        profile.update(dtype=rio.int16, count=1, compress="lzw", crs=crs)
        dst.write(data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute flow direction using D8 method")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-flowdir", "--flowdir-filename", help="Out filename", required=True, type=str)

    args = vars(parser.parse_args())

    flowdir_d8(**args)
