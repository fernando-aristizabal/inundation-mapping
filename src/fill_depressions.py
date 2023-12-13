#!/usr/bin/env python3

import argparse
import os

import rasterio as rio
import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def fill_depressions(
    dem_filename: str,
    out_filename: str,
    flat_increment: float = None,
    fix_flats: float = True,
    max_depth: float = None,
):
    """
    Fill depressions in DEM. Wrapper for WhiteboxTools fill_depressions.

    Parameters
    ----------
    dem_filename : str
        DEM filename
    out_filename : str
        Out filename
    fix_flats : bool
        Fix flats
    flat_increment : float
        Flat increment
    max_depth : float
        Max depth
    """

    assert os.path.isfile(dem_filename), 'ERROR: input DEM file not found: ' + str(dem_filename)

    # Fill depressions
    if wbt.fill_depressions(dem_filename, out_filename, fix_flats, flat_increment, max_depth) != 0:
        raise Exception('ERROR: WhiteboxTools fill_depressions failed')

    assert os.path.isfile(out_filename), 'ERROR: fill_depressions file not found: ' + str(out_filename)

    # Convert from double to float
    with rio.open(out_filename) as src:
        profile = src.profile
        profile.update(dtype=rio.float32, count=1)

        data = src.read(1).astype(rio.float32)

    # Write output
    with rio.open(out_filename, "w", **profile) as dst:
        dst.write(data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill depressions in DEM")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-out", "--out-filename", help="Out filename", required=True, type=str)
    parser.add_argument('-f', '--flat-increment', help='Flat increment', type=float, default=None)

    args = vars(parser.parse_args())

    fill_depressions(**args)
