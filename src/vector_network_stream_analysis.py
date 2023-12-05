#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import rasterio as rio
import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def vector_stream_network_analysis(
    dem_filename,
    d8_pntr_filename,
    stream_raster_filename,
    stream_id_filename,
    stream_length_filename,
    stream_slope_filename,
    stream_vector_filename,
    stream_vector_analysis_filename,
    pntr,
    zero_background=False,
):
    """
    Create vector stream network and add attributes like Strahler stream order
        1. D8 pointer: This tool calculates a D8 flow pointer raster from an input DEM.
        2. Raster streams to vector: This tool converts a raster stream file into a vector file.
        3. Repair stream vector topology: This tool repairs the topological errors in a vector stream network.
        4. Vector stream network analysis: This tool performs common stream network analysis operations on an
           input vector stream file.

    Parameters
    ----------
    dem_filename: str
        DEM filename
    d8_pntr_filename: str
        Flow accumulation filename
    stream_raster_filename: str
        Stream raster filename
    stream_vector_filename: str
        Stream vector filename
    """

    # Check input files
    assert os.path.isfile(dem_filename), 'ERROR: DEM file not found: ' + str(dem_filename)
    assert os.path.isfile(stream_raster_filename), 'ERROR: stream raster file not found: ' + str(
        stream_raster_filename
    )

    def d8_pointer(dem_filename, d8_pntr_filename, pntr):
        """
        Create flow direction raster from DEM

        Parameters
        ----------
        dem_filename: str
            DEM filename
        d8_pntr: str
            Flow accumulation filename
        pntr: str
            Output pointer mapping
            Options: 'wbt', 'TauDEM'
        """
        if pntr in ['TauDEM', 'wbt']:
            esri_pntr = False
        elif pntr == 'esri':
            esri_pntr = True

        # Compute flow direction
        if wbt.d8_pointer(dem_filename, d8_pntr_filename, esri_pntr) != 0:
            raise Exception('ERROR: WhiteboxTools d8_pointer failed')

        assert os.path.isfile(d8_pntr_filename), 'ERROR: flowdir file not found: ' + str(d8_pntr_filename)

        if pntr == 'TauDEM':
            # Reclassify WhiteboxTools flow direction to TauDEM flow direction
            with rio.open(d8_pntr_filename) as src:
                profile = src.profile
                nodata = src.nodata

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
            with rio.open(d8_pntr_filename, "w", **profile) as dst:
                profile.update(dtype=rio.int16, count=1, compress="lzw")
                dst.write(data, 1)

    def stream_link_identifier(
        d8_pntr, stream_raster_filename, stream_id_filename, pntr, zero_background=False
    ):
        if pntr in ['TauDEM', 'wbt']:
            esri_pntr = False
        elif pntr == 'esri':
            esri_pntr = True

        if (
            wbt.stream_link_identifier(
                d8_pntr, stream_raster_filename, stream_id_filename, esri_pntr, zero_background
            )
            != 0
        ):
            raise Exception('ERROR: WhiteboxTools stream_link_identifier failed')

    def stream_link_length(
        d8_pntr, stream_link_filename, stream_length_filename, pntr, zero_background=False
    ):
        if pntr in ['TauDEM', 'wbt']:
            esri_pntr = False
        elif pntr == 'esri':
            esri_pntr = True

        if (
            wbt.stream_link_length(
                d8_pntr, stream_link_filename, stream_length_filename, esri_pntr, zero_background
            )
            != 0
        ):
            raise Exception('ERROR: WhiteboxTools stream_link_length failed')

    def stream_link_slope(
        d8_pntr, stream_link_filename, dem_filename, stream_slope_filename, pntr, zero_background=False
    ):
        if pntr in ['TauDEM', 'wbt']:
            esri_pntr = False
        elif pntr == 'esri':
            esri_pntr = True

        if (
            wbt.stream_link_slope(
                d8_pntr, stream_link_filename, dem_filename, stream_slope_filename, esri_pntr, zero_background
            )
            != 0
        ):
            raise Exception('ERROR: WhiteboxTools stream_link_slope failed')

    def raster_streams_to_vector(stream_raster_filename, d8_pntr_filename, stream_vector_filename, pntr):
        if pntr in ['TauDEM', 'wbt']:
            esri_pntr = False
        elif pntr == 'esri':
            esri_pntr = True

        # Raster streams to vector
        if (
            wbt.raster_streams_to_vector(
                stream_raster_filename, d8_pntr_filename, stream_vector_filename, esri_pntr
            )
            != 0
        ):
            raise Exception('ERROR: WhiteboxTools raster_streams_to_vector failed')

        assert os.path.isfile(
            stream_vector_filename
        ), 'ERROR: stream vector file not created from raster_streams_to_vector: ' + str(
            stream_vector_filename
        )

    # def repair_stream_vector_topology(stream_vector_filename_in, stream_vector_filename_out, dist):
    #     # Repair stream vector topology
    #     if (
    #         wbt.repair_stream_vector_topology(stream_vector_filename_in, stream_vector_filename_out, dist)
    #         != 0
    #     ):
    #         raise Exception('ERROR: WhiteboxTools repair_stream_vector_topology failed')

    #     assert os.path.isfile(
    #         stream_vector_filename
    #     ), 'ERROR: stream vector file not created from repair_stream_vector_topology: ' + str(
    #         stream_vector_filename
    #     )

    def vector_stream_network_analysis(
        stream_vector_filename_in,
        dem_filename,
        stream_vector_filename_out,
        stream_id_filename,
        stream_length_filename,
        stream_slope_filename,
        cutting_height=10.0,
        snap=0.1,
    ):
        # Vector stream network analysis
        if (
            wbt.vector_stream_network_analysis(
                stream_vector_filename_in, dem_filename, stream_vector_filename_out, cutting_height, snap
            )
            != 0
        ):
            raise Exception('ERROR: WhiteboxTools vector_stream_network_analysis failed')

        assert os.path.isfile(
            stream_vector_filename
        ), 'ERROR: stream vector file not created from vector_stream_network_analysis: ' + str(
            stream_vector_filename
        )

        # Add stream attributes to stream vector
        stream_vector = gpd.read_file(stream_vector_filename_out)
        stream_vector_in = gpd.read_file(stream_vector_filename_in)

        stream_vector_in['STRM_VAL'] = stream_vector_in['STRM_VAL'].astype(int)

        stream_vector = stream_vector.merge(stream_vector_in[['FID', 'STRM_VAL']], on='FID', how='left')

        # Update stream vector attributes to TauDEM convention
        stream_vector['LINKNO'] = stream_vector['FID'].astype(int)
        stream_vector['DSLINKNO'] = stream_vector['DS_LINK_ID'].astype(int) + 1
        stream_vector['strmOrder'] = stream_vector['STRAHLER'].astype(int)

        stream_vector = stream_vector[['FID', 'LINKNO', 'DSLINKNO', 'strmOrder', 'geometry']]

        # Read stream link identifier, length, and slope rasters
        with rio.open(stream_id_filename) as id_src, rio.open(stream_length_filename) as length_src, rio.open(
            stream_slope_filename
        ) as slope_src:
            id = id_src.read(1)
            length = length_src.read(1)
            slope = slope_src.read(1)

        # Get length and slope from rasters for each stream link
        lengths = list(set(zip(id[id > 0], length[id > 0])))
        slopes = list(set(zip(id[id > 0], slope[id > 0])))

        stream_vector['length'] = stream_vector['FID'].map(dict(lengths))
        stream_vector['slope'] = stream_vector['FID'].map(dict(slopes))

        stream_vector.to_file(stream_vector_analysis_filename, driver='GeoJSON')

    # Create flow direction raster
    d8_pointer(dem_filename, d8_pntr_filename, pntr)

    # Stream link identifier
    stream_link_identifier(
        d8_pntr_filename, stream_raster_filename, stream_id_filename, pntr, zero_background=False
    )

    # Stream link length
    stream_link_length(
        d8_pntr_filename, stream_id_filename, stream_length_filename, pntr, zero_background=False
    )

    # Stream link slope
    stream_link_slope(
        d8_pntr_filename, stream_id_filename, dem_filename, stream_slope_filename, pntr, zero_background=False
    )

    # Raster streams to vector
    raster_streams_to_vector(stream_id_filename, d8_pntr_filename, stream_vector_filename, pntr)

    # Repair stream vector topology
    # repair_stream_vector_topology(stream_vector_filename, stream_vector_filename, dist=1.0)

    # Vector stream network analysis
    vector_stream_network_analysis(
        stream_vector_filename,
        dem_filename,
        stream_vector_analysis_filename,
        stream_id_filename,
        stream_length_filename,
        stream_slope_filename,
        cutting_height=10.0,
        snap=1.0,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process streams")
    parser.add_argument("-dem", "--dem-filename", help="d8_pntr filename", required=True, type=str)
    parser.add_argument("-d8_pntr", "--d8-pntr-filename", help="d8_pntr filename", required=True, type=str)
    parser.add_argument(
        "-streams", "--stream-raster-filename", help="Stream raster filename", required=True, type=str
    )
    parser.add_argument(
        "-id", "--stream-id-filename", help="Stream link identifier filename", required=True, type=str
    )
    parser.add_argument(
        "-length", "--stream-length-filename", help="Stream link length filename", required=True, type=str
    )
    parser.add_argument(
        "-slope", "--stream-slope-filename", help="Stream link slope filename", required=True, type=str
    )
    parser.add_argument(
        "-vector", "--stream-vector-filename", help="Stream vector filename", required=True, type=str
    )
    parser.add_argument(
        "-analysis",
        "--stream-vector-analysis-filename",
        help="Stream vector analysis filename",
        required=True,
        type=str,
    )
    parser.add_argument("-pntr", "--pntr", help="Pointer", required=False, default='wbt', type=str)
    parser.add_argument("-zero", "--zero-background", help="Zero background", action="store_true")

    args = vars(parser.parse_args())

    vector_stream_network_analysis(**args)
