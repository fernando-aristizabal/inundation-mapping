#!/usr/bin/env python3

import argparse
import os

import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def vector_stream_network_analysis(
    dem_filename,
    d8_pntr_filename,
    stream_raster_filename,
    stream_vector_filename,
    esri_pntr=False,
    zero_background=False,
):
    """
    Create vector stream network and add attributes like Strahler stream order
        1. Raster streams to vector: This tool converts a raster stream file into a vector file.
        2. Repair stream vector topology: This tool repairs the topological errors in a vector stream network.
        3. Vector stream network analysis: This tool performs common stream network analysis operations on an
           input vector stream file.

    Parameters
    ----------
    dem_filename: str
        DEM filename
    d8_pntr: str
        Flow accumulation filename
    stream_raster_filename: str
        Stream raster filename
    stream_vector_filename: str
        Stream vector filename
    """

    assert os.path.isfile(dem_filename), 'ERROR: DEM file not found: ' + str(dem_filename)
    assert os.path.isfile(d8_pntr_filename), 'ERROR: d8_pntr file not found: ' + str(d8_pntr_filename)
    assert os.path.isfile(stream_raster_filename), 'ERROR: stream raster file not found: ' + str(
        stream_raster_filename
    )

    # Raster streams to vector
    if (
        wbt.raster_streams_to_vector(
            stream_raster_filename, d8_pntr_filename, stream_vector_filename, esri_pntr
        )
        != 0
    ):
        raise Exception('ERROR: WhiteboxTools raster_streams_to_vector failed')

    # Repair stream vector topology
    if wbt.repair_stream_vector_topology(stream_vector_filename, stream_vector_filename, dist="") != 0:
        raise Exception('ERROR: WhiteboxTools repair_stream_vector_topology failed')

    # Vector stream network analysis
    if (
        wbt.vector_stream_network_analysis(
            stream_vector_filename, dem_filename, stream_vector_filename, cutting_height=10.0, snap=0.1
        )
        != 0
    ):
        raise Exception('ERROR: WhiteboxTools vector_stream_network_analysis failed')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Strahler stream order")
    parser.add_argument("-dem", "--dem-filename", help="d8_pntr filename", required=True, type=str)
    parser.add_argument("-d8_pntr", "--d8-pntr-filename", help="d8_pntr filename", required=True, type=str)
    parser.add_argument(
        "-streams", "--stream-raster-filename", help="Stream raster filename", required=True, type=str
    )
    parser.add_argument("-out", "--out-filename", help="Out filename", required=True, type=str)
    parser.add_argument("-zero", "--zero-background", help="Zero background", action="store_true")

    args = vars(parser.parse_args())

    vector_stream_network_analysis(**args)
