from typing import List

from shapely.affinity import translate
from shapely.geometry import MultiPolygon, Polygon


def poly_coords_parsing(a_string: str) -> List[List[float]]:
    """Used to parse a string with coordinates and convert it to a list of points (latitude, longitude)

    Parameters
    ----------
    a_string: str
        String (xpath ouput usually) with posList coordinates (separated by a white space)

    Returns
    ----------
    List[List[float]]
        List containg pairs of coordinates casted to floating point representation (latitude, longitude)
    """
    # remove begining and ending spaces
    a_string.strip()
    # remove inner empty spaces
    clean_string = filter(None, a_string.split(" "))
    iter_string = iter(clean_string)
    # Create pairs [float, float] with latitudes and longitudes
    return [[float(lon), float(lat)] for lat, lon in zip(iter_string, iter_string)]


def detect_pole_or_antemeridian(coordinates: List[List[float]]) -> bool:
    """Verify if a list of coordinates crosses a antemeridian or a pole

    Parameters
    ----------
    coordinates: List[List[float]]
        List containg pairs of coordinates (latitude, longitude)

    Returns
    ----------
    bool:
        True if coordinates croos pole/antemeridian at least once, False otherwise
    """
    import itertools

    longitude_threshold = 270
    crossing = 0
    # Flat coordinates in order to iterate only over longitudes
    flatten_coords = [longitude for longitude in itertools.chain.from_iterable(coordinates)]
    # Compare absolute difference of longitude[i+1], longitude[i] with threshold
    for current_longitude, next_longitude in zip(flatten_coords[1::2], flatten_coords[3::2]):
        longitude_difference = abs(next_longitude - current_longitude)
        if longitude_difference > longitude_threshold:
            crossing += 1

    return crossing >= 1


def split_poly(polygon: Polygon) -> MultiPolygon:
    the_planet = Polygon([[-180, 90], [180, 90], [180, -90], [-180, -90], [-180, 90]])
    shifted_planet = Polygon([[180, 90], [540, 90], [540, -90], [180, -90], [180, 90]])
    normalized_points = []
    for point in polygon.exterior.coords:
        lon = point[0]
        if lon < 0.0:
            lon += 360.0
        normalized_points.append([lon, point[1]])

    normalized_polygon = Polygon(normalized_points)

    # cut out eastern part (up to 180 deg)
    intersection_east = the_planet.intersection(normalized_polygon)

    # cut out western part - shifted by 360 deg using the shifted planet boundary
    # and shift the intersection back westwards to the -180-> 180 deg range
    intersection_west = shifted_planet.intersection(normalized_polygon)
    shifted_back = translate(intersection_west, -360.0, 0, 0)

    return MultiPolygon([intersection_east, shifted_back])
