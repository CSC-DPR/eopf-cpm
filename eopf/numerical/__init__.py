"""
The objective of the eopf.numerical module is to provide the mathematical
functions necessary for the implementation of the re-engineered processors.

The signal processing functions that are provided can be grouped in different areas:

    * Fourier transforms
    * Filtering
    * Interpolation
    * Fitting

Different high-level functionalities can then be built around the provided numerical functions.

The defined functions shall provide a uniform access to data stored as
dask arrays and take advantage of chunks whenever feasible, without explicit intervention of the user.
Functionalities will be provided as wrappers to best-in-class implementations in python modules, when available.
Note that function wrappers provided in numerical module could be updated to any new equivalent implementation
that proves to be more efficient when needed.
"""
