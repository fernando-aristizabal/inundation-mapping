#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def vector_stream_network_analysis(
    dem_filename,
    d8_pntr_filename,
    stream_raster_filename,
    stream_id_filename,
    stream_vector_filename,
    stream_vector_analysis_filename,
    pntr,
    zero_background=False,
):
    """
    Create vector stream network and add attributes like Strahler stream order
        1. Stream link identifier: This tool identifies the links, or tributary segments, in a stream network.
        2. Raster streams to vector: This tool converts a raster stream file into a vector file.
        3. Vector stream network analysis: This tool performs common stream network analysis operations on an
           input vector stream file.

    Parameters
    ----------
    dem_filename: str
        DEM filename
    d8_pntr_filename: str
        Flow direction filename
    stream_raster_filename: str
        Stream raster filename
    stream_vector_filename: str
        Stream vector filename
    stream_vector_analysis_filename: str
        Stream vector analysis filename
    pntr: str
        Output pointer mapping
    zero_background: bool
        Zero background value
    """

    # Check input files
    assert os.path.isfile(d8_pntr_filename), 'ERROR: input D8 flow direction file not found: ' + str(
        d8_pntr_filename
    )
    assert os.path.isfile(dem_filename), 'ERROR: input DEM file not found: ' + str(dem_filename)
    assert os.path.isfile(stream_raster_filename), 'ERROR: input stream raster file not found: ' + str(
        stream_raster_filename
    )

    def stream_link_identifier(
        d8_pntr_filename, stream_raster_filename, stream_id_filename, pntr, zero_background
    ):
        """
        This tool identifies the links, or tributary segments, in a stream network. Wrapper for WhiteboxTools stream_link_identifier.

        Parameters
        ----------
        d8_pntr_filename: str
            Flow direction filename
        stream_raster_filename: str
            Stream raster filename
        stream_id_filename: str
            Stream link identifier filename
        pntr: str
            Output pointer mapping
        zero_background: bool
            Zero background value
        """

        if pntr in ['TauDEM', 'wbt']:
            esri_pntr = False
        elif pntr == 'esri':
            esri_pntr = True

        if (
            wbt.stream_link_identifier(
                d8_pntr_filename, stream_raster_filename, stream_id_filename, esri_pntr, zero_background
            )
            != 0
        ):
            raise Exception('ERROR: WhiteboxTools stream_link_identifier failed')

        assert os.path.isfile(
            stream_id_filename
        ), 'ERROR: stream_link_identifier file not created from stream_link_identifier: ' + str(
            stream_id_filename
        )

    def raster_streams_to_vector(stream_raster_filename, d8_pntr_filename, stream_vector_filename, pntr):
        """
        This tool converts a raster stream file into a vector file. Wrapper for WhiteboxTools raster_streams_to_vector

        Parameters
        ----------
        stream_raster_filename: str
            Stream raster filename
        d8_pntr_filename: str
            Flow accumulation filename
        stream_vector_filename: str
            Stream vector filename
        pntr: str
            Output pointer mapping
        """

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

    def vector_stream_network_analysis(
        stream_vector_filename,
        dem_filename,
        stream_vector_analysis_filename,
        cutting_height=10.0,
        snap=0.1,
        crs='epsg:5070',
    ):
        """
        This tool performs common stream network analysis operations on an input vector stream file. Wrapper for WhiteboxTools vector_stream_network_analysis.

        Parameters
        ----------
        stream_vector_filename: str
            Stream vector filename
        dem_filename: str
            DEM filename
        stream_vector_analysis_filename: str
            Stream vector analysis filename
        cutting_height: float
            Cutting height
        snap: float
            Snap distance
        crs: str
            Coordinate reference system
        """

        # Vector stream network analysis
        if (
            wbt.vector_stream_network_analysis(
                stream_vector_filename, dem_filename, stream_vector_analysis_filename, cutting_height, snap
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
        stream_vector = gpd.read_file(stream_vector_analysis_filename)
        stream_vector_in = gpd.read_file(stream_vector_filename)

        stream_vector_in['STRM_VAL'] = stream_vector_in['STRM_VAL'].astype(int)

        # Update stream vector attributes to TauDEM streamnet convention
        stream_vector['LINKNO'] = stream_vector['FID'].astype(int)
        stream_vector['DSLINKNO'] = stream_vector['DS_LINK_ID'].astype(int) + 1
        stream_vector['strmOrder'] = stream_vector['STRAHLER'].astype(int)

        stream_vector['LINKNO'] = stream_vector['LINKNO'].replace(
            dict(zip(stream_vector_in['FID'], stream_vector_in['STRM_VAL']))
        )
        stream_vector['DSLINKNO'] = stream_vector['DSLINKNO'].replace(
            dict(zip(stream_vector_in['FID'], stream_vector_in['STRM_VAL']))
        )

        stream_vector = stream_vector[['FID', 'LINKNO', 'DSLINKNO', 'strmOrder', 'geometry']]

        stream_vector.to_file(stream_vector_filename, crs=crs)

    ### Main routines called below ###

    # Stream link identifier
    stream_link_identifier(
        d8_pntr_filename, stream_raster_filename, stream_id_filename, pntr, zero_background
    )

    # Raster streams to vector
    raster_streams_to_vector(stream_id_filename, d8_pntr_filename, stream_vector_filename, pntr)

    # Vector stream network analysis
    vector_stream_network_analysis(
        stream_vector_filename,
        dem_filename,
        stream_vector_analysis_filename,
        cutting_height=10.0,
        snap=1.0,
        crs='epsg:5070',
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
